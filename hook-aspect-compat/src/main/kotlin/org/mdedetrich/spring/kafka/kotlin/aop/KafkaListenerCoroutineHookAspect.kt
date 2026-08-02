package org.mdedetrich.spring.kafka.kotlin.aop

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.core.KotlinDetector
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.HandlerMethod
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.util.ConcurrentReferenceHashMap
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.kotlinFunction

/**
 * Runs suspend `@KafkaListener` methods through a [KafkaListenerCoroutineHook] -- the backport of
 * `hook-aspect-native`'s Aspect for spring-kafka before 3.2 (2.8/2.9/3.0/3.1). Public API is identical to
 * that module's; the internal invocation mechanism is deliberately different. See
 * `hook-aspect-compat/README.md` for the full story; summarized:
 *
 * spring-kafka's own suspend-`@KafkaListener` support doesn't exist before 3.2 -- the listener
 * container's argument-resolution step has no resolver for the compiler-generated trailing
 * [kotlin.coroutines.Continuation] parameter every suspend function actually has, and fails before the method is ever
 * invoked. [KafkaListenerCoroutineMessageHandlerMethodFactory] fixes that one step (consumers must
 * register it as a bean -- see its own doc).
 *
 * That still leaves this Aspect's own invocation mechanism: `hook-aspect-native`'s version hijacks the real
 * [kotlin.coroutines.Continuation], launches an async coroutine, and returns `COROUTINE_SUSPENDED`, relying on Spring
 * Framework's own suspend-aware AOP proxy bridging ([org.springframework.aop.framework.CoroutinesUtils])
 * to correctly propagate that back to the real caller. That proxy-level suspend-awareness is a *Spring
 * Framework* feature (not spring-kafka's), and it doesn't exist on the Spring Framework versions
 * spring-kafka 2.8/2.9/3.0 depend on (5.3.x, 6.0.x) -- confirmed directly against each version's
 * `spring-aop` sources, not assumed. Only spring-kafka 3.1's Spring Framework version (6.1.9) actually
 * has it.
 *
 * So this Aspect never relies on it, on any of the 4 versions this module supports: [hook] and
 * `processMessage` run synchronously, and this method returns the real computed result (or throws the
 * real exception) directly -- never `COROUTINE_SUSPENDED`. A suspend function completing synchronously
 * (as opposed to genuinely suspending) is a normal, legal outcome of the suspend calling convention, so
 * this works correctly regardless of whether the underlying AOP proxy layer has any suspend-specific
 * handling at all. It's also not a real behavioral downgrade for this specific use case: spring-kafka's
 * listener container is a traditional poll-based, single-thread-per-partition consumer loop, so the
 * container thread is already fully occupied for the whole listener invocation either way.
 *
 * The synchronous bridge itself is plain [kotlinx.coroutines.runBlocking], not
 * `runBlocking(Dispatchers.Unconfined)`: this must behave as an ordinary synchronous method call to
 * whatever called [aroundKafkaListener], including returning on the same thread it was called on, even if
 * [hook] relocates `processMessage` onto a different dispatcher internally (e.g.
 * `withContext(Dispatchers.IO) { processMessage() }`). Plain [runBlocking]'s own event-loop dispatcher
 * guarantees exactly that; [kotlinx.coroutines.Dispatchers.Unconfined] would not. A lighter [kotlin.coroutines.startCoroutine] +
 * [java.util.concurrent.CountDownLatch]-based bridge was prototyped and benchmarked (see `hook-aspect-compat/README.md`), but
 * the measured win was small (~3%, close to the noise floor) relative to the added complexity, so this
 * stayed on plain [runBlocking].
 *
 * One consequence: unlike `hook-aspect-native`'s Aspect, this one has nothing running after
 * [aroundKafkaListener] returns -- the whole invocation has already completed (or thrown) by then, so
 * there's no in-flight work a `DisposableBean.destroy()` would need to cancel. This class deliberately
 * doesn't implement [org.springframework.beans.factory.DisposableBean] for that reason; it's an internal lifecycle detail of the async
 * design, not part of this feature's public contract.
 *
 * If [hook] is exactly [KafkaListenerCoroutineHook.NONE], this Aspect steps aside entirely (plain
 * `proceed()`) without even reflecting on the method -- a recognized fast path for "this listener isn't
 * wrapped at all," rather than invoking a hook we already know just calls straight through.
 *
 * @param hook wraps each suspend listener invocation.
 */
@Aspect
public class KafkaListenerCoroutineHookAspect(
    private val hook: KafkaListenerCoroutineHook,
) {
    /**
     * Composes multiple hooks into one, the same way Spring WebFlux composes multiple `CoWebFilter` beans
     * into its `WebFilterChain`: [hooks]'s first element is outermost (its "before [processMessage]" logic
     * runs first, its "after" logic runs last), each subsequent hook nested inside it, with the real
     * listener body innermost.
     *
     * This is also how ordering multiple hooks with `@Order`/[org.springframework.core.Ordered] works:
     * declare each hook as its own `@Order`-annotated Spring bean (exactly like `@Order(Ordered
     * .HIGHEST_PRECEDENCE) class MyHook : KafkaListenerCoroutineHook()`), and inject them collected into
     * a `List<KafkaListenerCoroutineHook>` -- Spring's `AnnotationAwareOrderComparator` sorts any such
     * list/array-typed bean dependency by `@Order`/`Ordered` automatically before this constructor ever
     * sees it, no extra code needed on [KafkaListenerCoroutineHook] itself.
     * @param hooks the hooks to compose, outermost first.
     */
    public constructor(hooks: List<KafkaListenerCoroutineHook>) : this(hooks.compose())

    @Around("@annotation(org.springframework.kafka.annotation.KafkaListener)")
    public fun aroundKafkaListener(pjp: ProceedingJoinPoint): Any? {
        val method = (pjp.signature as MethodSignature).method
        if (!KotlinDetector.isSuspendingFunction(method) || hook === KafkaListenerCoroutineHook.NONE) {
            return pjp.proceed()
        }
        val metadata = listenerMethodMetadata(method)
        val kFunction = metadata.kFunction ?: return pjp.proceed()
        val bean = pjp.target
        val handlerMethod = handlerMethodFor(bean, method)
        val args = pjp.args
        val businessArgs = args.copyOfRange(0, args.size - 1)
        val invocation = classifyInvocation(handlerMethod, businessArgs, metadata)
        val processMessage: suspend () -> Any? = { kFunction.callSuspend(bean, *businessArgs) }

        // Plain runBlocking, not runBlocking(Dispatchers.Unconfined): this must behave as an ordinary
        // synchronous method call to whatever called aroundKafkaListener (spring-kafka's own container,
        // reflectively, on pre-3.2), including returning on the same thread it was called on. Plain
        // runBlocking's own event-loop dispatcher guarantees exactly that, even if hook internally hops
        // across dispatchers via withContext -- Dispatchers.Unconfined would not.
        return try {
            runBlocking { hook.hook(invocation, processMessage) }
        } catch (e: InvocationTargetException) {
            // kotlin-reflect's callSuspend invokes the real method via java.lang.reflect.Method under the
            // hood, wrapping whatever the listener body actually threw in an InvocationTargetException --
            // unwrap it so the caller sees the real cause.
            throw e.cause ?: e
        }
    }
}

// Which of KafkaListenerInvocation's shapes a listener method takes, and (for IndividualParameters) which
// parameter index carries @Payload/each @Header -- everything classifyInvocation would otherwise have to
// re-derive via reflection on every single message, even though none of it depends on the runtime
// argument values, only on the method's declaration.
private sealed class ListenerShape {
    data class SingleRecord(
        val recordIndex: Int,
    ) : ListenerShape()

    data class BatchRecords(
        val recordsIndex: Int,
    ) : ListenerShape()

    data class IndividualParameters(
        val payloadIndex: Int?,
        val headerIndices: Map<String, Int>,
    ) : ListenerShape()
}

// Everything about a suspend @KafkaListener method that's invariant across every message it ever
// receives: resolved once per Method (via listenerMethodMetadata's cache) rather than on every
// invocation, since kotlin-reflect's Method.kotlinFunction and the JDK's Method.getGenericParameterTypes/
// getParameterAnnotations (which allocate a fresh array on every call) are otherwise redone per message.
private class ListenerMethodMetadata(
    val kFunction: KFunction<*>?,
    val acknowledgmentIndex: Int?,
    val consumerIndex: Int?,
    val shape: ListenerShape,
)

// Soft-keyed (not a plain ConcurrentHashMap): a Method strongly references its declaring Class, which
// references its ClassLoader -- a plain map would pin the ClassLoader of every listener class this
// Aspect has ever seen for the JVM's lifetime, leaking across hot-redeploy scenarios (e.g. Spring
// DevTools restarts) that load the same listener class under a new ClassLoader. Same fix
// ReflectionUtils/AnnotationUtils use internally for the identical problem shape.
private val methodMetadataCache = ConcurrentReferenceHashMap<Method, ListenerMethodMetadata>()

private fun listenerMethodMetadata(method: Method): ListenerMethodMetadata =
    // computeIfAbsent's return type is a @Nullable V for the general case of a mapping function that
    // itself returns null (meaning "don't cache anything") -- buildListenerMethodMetadata never does.
    methodMetadataCache.computeIfAbsent(method, ::buildListenerMethodMetadata)!!

// HandlerMethod(bean, method) does real reflective work on every construction (BridgeMethodResolver,
// building a MethodParameter[] with its own annotation/generic-type introspection per parameter) despite
// depending only on which method/bean pair it wraps -- confirmed via a dedicated benchmark, not assumed.
// Keyed on (method, bean identity) rather than method alone: the overwhelming majority of listener beans
// are singletons (one bean per method for the Aspect's lifetime, so this cache holds effectively one
// entry per method in practice), but a prototype-scoped listener bean is valid Spring usage where bean
// really can vary across invocations of the same method, and reusing a stale HandlerMethod would report
// the wrong bean.
private class HandlerMethodKey(
    private val method: Method,
    private val bean: Any,
) {
    override fun equals(other: Any?): Boolean = other is HandlerMethodKey && method == other.method && bean === other.bean

    override fun hashCode(): Int = 31 * method.hashCode() + System.identityHashCode(bean)
}

private val handlerMethodCache = ConcurrentReferenceHashMap<HandlerMethodKey, HandlerMethod>()

private fun handlerMethodFor(
    bean: Any,
    method: Method,
): HandlerMethod = handlerMethodCache.computeIfAbsent(HandlerMethodKey(method, bean)) { HandlerMethod(bean, method) }!!

// Classifies from the method's *declared* parameter types, not the runtime argument values -- an empty
// batch (List<ConsumerRecord<K, V>>) can't be told apart from an empty List<String> by inspecting the
// value alone, but the declared generic parameter type always says which one it is. Acknowledgment and
// Consumer are plain concrete (non-generic) types, so the same declared-type check unambiguously locates
// them too, with no equivalent of the batch-shape erasure problem.
private fun buildListenerMethodMetadata(method: Method): ListenerMethodMetadata {
    val kFunction = method.kotlinFunction?.apply { isAccessible = true }
    val paramTypes = method.genericParameterTypes
    val rawTypes = paramTypes.map(::rawClassOf)

    val acknowledgmentIndex = rawTypes.indexOfFirst { it != null && Acknowledgment::class.java.isAssignableFrom(it) }.takeIf { it >= 0 }
    val consumerIndex = rawTypes.indexOfFirst { it != null && Consumer::class.java.isAssignableFrom(it) }.takeIf { it >= 0 }

    for (i in paramTypes.indices) {
        val rawType = rawTypes[i] ?: continue
        if (ConsumerRecord::class.java.isAssignableFrom(rawType)) {
            return ListenerMethodMetadata(kFunction, acknowledgmentIndex, consumerIndex, ListenerShape.SingleRecord(i))
        }
        if (Collection::class.java.isAssignableFrom(rawType)) {
            val elementType = (paramTypes[i] as? ParameterizedType)?.actualTypeArguments?.firstOrNull()
            val elementRawType = elementType?.let(::rawClassOf)
            if (elementRawType != null && ConsumerRecord::class.java.isAssignableFrom(elementRawType)) {
                return ListenerMethodMetadata(kFunction, acknowledgmentIndex, consumerIndex, ListenerShape.BatchRecords(i))
            }
        }
    }

    var payloadIndex: Int? = null
    val headerIndices = mutableMapOf<String, Int>()
    val parameterAnnotations = method.parameterAnnotations
    for (i in parameterAnnotations.indices) {
        for (annotation in parameterAnnotations[i]) {
            when (annotation) {
                is Payload -> payloadIndex = i
                is Header -> headerIndices[annotation.value] = i
            }
        }
    }
    return ListenerMethodMetadata(
        kFunction,
        acknowledgmentIndex,
        consumerIndex,
        ListenerShape.IndividualParameters(payloadIndex, headerIndices),
    )
}

@Suppress("UNCHECKED_CAST")
private fun classifyInvocation(
    handlerMethod: HandlerMethod,
    businessArgs: Array<Any?>,
    metadata: ListenerMethodMetadata,
): KafkaListenerInvocation {
    // asList(), not toList(): a zero-copy view over businessArgs rather than an eager copy -- safe since
    // businessArgs is never mutated after this point, and avoids paying for a copy of "the least likely
    // field a hook actually needs" (see KafkaListenerInvocation's doc) on every single message.
    val methodArgumentValues = businessArgs.asList()
    val acknowledgment = metadata.acknowledgmentIndex?.let { businessArgs[it] as Acknowledgment? }
    val consumer = metadata.consumerIndex?.let { businessArgs[it] as Consumer<*, *>? }
    return when (val shape = metadata.shape) {
        is ListenerShape.SingleRecord ->
            KafkaListenerInvocation.SingleRecord(
                handlerMethod,
                acknowledgment,
                consumer,
                businessArgs[shape.recordIndex] as ConsumerRecord<*, *>,
                methodArgumentValues,
            )
        is ListenerShape.BatchRecords ->
            KafkaListenerInvocation.BatchRecords(
                handlerMethod,
                acknowledgment,
                consumer,
                businessArgs[shape.recordsIndex] as List<ConsumerRecord<*, *>>,
                methodArgumentValues,
            )
        is ListenerShape.IndividualParameters -> {
            val payload = shape.payloadIndex?.let { businessArgs[it] }
            val headers = shape.headerIndices.mapValues { (_, index) -> businessArgs[index] }
            KafkaListenerInvocation.IndividualParameters(handlerMethod, acknowledgment, consumer, payload, headers, methodArgumentValues)
        }
    }
}

private fun rawClassOf(type: Type): Class<*>? =
    when (type) {
        is Class<*> -> type
        is ParameterizedType -> type.rawType as? Class<*>
        // Kotlin's read-only List<T> is declaration-site covariant, so List<ConsumerRecord<K, V>>
        // compiles to Java's List<? extends ConsumerRecord<K, V>> -- an upper-bounded wildcard, not a
        // bare Class or ParameterizedType.
        is WildcardType -> type.upperBounds.firstOrNull()?.let(::rawClassOf)
        else -> null
    }

// Right-fold into a single hook: the first element ends up outermost (its "before"/"after" logic wraps
// everything after it), each subsequent hook nested one level deeper, with the real processMessage
// (passed in at call time, not here) innermost -- the same nesting order a WebFilterChain builds from
// multiple CoWebFilter beans. An empty list composes to NONE itself (not a no-op wrapper around nothing),
// preserving aroundKafkaListener's `hook === KafkaListenerCoroutineHook.NONE` reference-equality fast
// path; a singleton list likewise reduces to that one element unchanged, for the same reason.
private fun List<KafkaListenerCoroutineHook>.compose(): KafkaListenerCoroutineHook =
    if (isEmpty()) {
        KafkaListenerCoroutineHook.NONE
    } else {
        reduceRight { outer, inner ->
            object : KafkaListenerCoroutineHook() {
                override suspend fun hook(
                    invocation: KafkaListenerInvocation,
                    processMessage: suspend () -> Any?,
                ): Any? = outer.hook(invocation) { inner.hook(invocation, processMessage) }
            }
        }
    }
