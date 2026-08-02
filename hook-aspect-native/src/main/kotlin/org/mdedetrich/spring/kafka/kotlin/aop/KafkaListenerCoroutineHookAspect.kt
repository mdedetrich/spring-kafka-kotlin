package org.mdedetrich.spring.kafka.kotlin.aop

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.beans.factory.DisposableBean
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
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.startCoroutine
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.kotlinFunction

/**
 * Runs suspend `@KafkaListener` methods through a [KafkaListenerCoroutineHook] -- inspired by Spring
 * WebFlux's `CoWebFilter`, though named `Hook` rather than `Filter`: see [KafkaListenerCoroutineHook] for
 * why. See [KafkaListenerCoroutineHook] for the intended use (e.g. propagating a correlation id extracted
 * from the incoming record's headers, or running the listener on a specific dispatcher).
 *
 * `spring-kafka` already detects suspend `@KafkaListener` methods itself (via
 * [org.springframework.kafka.listener.adapter.KotlinAwareInvocableHandlerMethod]), invoking them via
 * [org.springframework.core.CoroutinesUtils.invokeSuspendingFunction], which always starts the coroutine
 * on [Dispatchers.Unconfined] -- meaning the listener body starts running on the Kafka listener
 * container's own thread and stays there until its first real suspension point. There's no built-in hook
 * to wrap that invocation.
 *
 * This Aspect intercepts the *proxy* invocation of the listener method (so the listener bean must be a
 * Spring-managed bean that AOP can proxy -- typically automatic for `@Component`/`@Service` classes with
 * the `kotlin("plugin.spring")` Gradle plugin applied, which opens them for CGLIB), and, for suspend
 * methods, hijacks the real [kotlin.coroutines.Continuation] supplied by the framework: it starts
 * [hook] (via [kotlin.coroutines.startCoroutine], not [kotlinx.coroutines.launch] -- see the field-level doc on [scope])
 * on the hijacked continuation's own [kotlin.coroutines.CoroutineContext] (so any context elements the
 * caller already had are preserved) with [Dispatchers.Unconfined] forced on top (matching `spring-kafka`'s
 * own default -- the hook is free to move elsewhere itself, e.g. via [kotlinx.coroutines.withContext]), passing it a
 * `processMessage` suspend function that invokes the real listener body via Kotlin reflection
 * ([kotlin.reflect.full.callSuspend]). Once [hook] returns (or throws), the original continuation is
 * resumed with that result, and this method immediately returns [COROUTINE_SUSPENDED] to signal "still
 * pending" back up the call chain -- the same signal a genuinely suspending call would produce. Spring's
 * own suspend-aware AOP proxy machinery ([org.springframework.aop.framework.CglibAopProxy]/[org.springframework.aop.framework.JdkDynamicAopProxy], both
 * [KotlinDetector.isSuspendingFunction]-aware) bridges that back into the caller's real coroutine
 * transparently; this was verified directly (not assumed) against a running Spring context before relying
 * on it.
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
) : DisposableBean {
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

    // Not used to launch invocations directly (see aroundKafkaListener) -- exists purely to hold the
    // parent Job that every per-invocation Job is created as a child of, so destroy()'s single
    // scope.cancel() cancels every invocation still in flight, without paying for a full launch() per
    // message.
    private val scope = CoroutineScope(SupervisorJob())

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

        @Suppress("UNCHECKED_CAST")
        val continuation = args.last() as Continuation<Any?>
        val processMessage: suspend () -> Any? = { kFunction.callSuspend(bean, *businessArgs) }

        // A real, cancellable Job per invocation (a child of `scope`'s own SupervisorJob, so destroy()'s
        // scope.cancel() still cancels every invocation still in flight -- cooperative cancellation checks
        // at suspension points key off whichever Job is present in the coroutine's own context, regardless
        // of how the coroutine was started), but started via the stdlib's startCoroutine rather than
        // kotlinx.coroutines' launch -- avoiding launch's heavier StandaloneCoroutine wrapper (structured-
        // concurrency bookkeeping this single-invocation, fire-and-forget use case doesn't need: no
        // children of its own, no result to expose, nothing awaiting completion via the Job itself).
        // Propagates the caller's own CoroutineContext elements (e.g. a tracing/MDC ThreadContextElement)
        // too, minus the caller's Job (replaced by our own invocationJob) -- Dispatchers.Unconfined still
        // wins over whatever ContinuationInterceptor the caller's context may have had.
        val invocationJob = Job(parent = scope.coroutineContext[Job])
        val invocationContext = continuation.context.minusKey(Job) + Dispatchers.Unconfined + invocationJob
        val block: suspend () -> Any? = {
            try {
                hook.hook(invocation, processMessage)
            } catch (e: Throwable) {
                // kotlin-reflect's callSuspend invokes the real method via java.lang.reflect.Method
                // under the hood, wrapping whatever the listener body actually threw in an
                // InvocationTargetException -- unwrap it so the caller sees the real cause, exactly as
                // spring-kafka's own CoroutinesUtils.invokeSuspendingFunction would.
                throw if (e is InvocationTargetException) e.cause ?: e else e
            }
        }
        block.startCoroutine(
            Continuation(invocationContext) { result ->
                invocationJob.complete()
                continuation.resumeWith(result)
            },
        )
        return COROUTINE_SUSPENDED
    }

    /** Cancels any listener invocations still in flight when the owning Spring context is destroyed. */
    override fun destroy() {
        scope.cancel()
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
