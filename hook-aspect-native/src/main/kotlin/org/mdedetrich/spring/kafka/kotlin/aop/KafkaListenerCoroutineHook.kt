package org.mdedetrich.spring.kafka.kotlin.aop

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.HandlerMethod

/**
 * Everything about an intercepted suspend `@KafkaListener` invocation that [KafkaListenerCoroutineHook]
 * might need, already picked apart into the shape that actually matters -- rather than a raw
 * `Array<Any?>` the hook would otherwise have to inspect and cast itself.
 *
 * `@KafkaListener` methods can be declared in several different shapes, determined from the method's own
 * declared parameter types (not by guessing from the runtime arguments, which can't distinguish, say, an
 * empty `List<ConsumerRecord<K, V>>` from an empty `List<String>`). This is a sealed hierarchy so a
 * [KafkaListenerCoroutineHook] can `when`-match over the cases it cares about exhaustively, rather than
 * defensively null-checking a single flattened shape.
 *
 * [acknowledgment] and [consumer] are declared on the base type rather than per-case: [Acknowledgment]
 * (manual ack mode) and `Consumer<?, ?>` (raw consumer access) parameters are valid alongside *any* of
 * the record shapes below, not just one of them.
 *
 * @property handlerMethod the suspend `@KafkaListener` method about to be invoked, bundled with its real
 * target bean (not the AOP proxy) -- the same abstraction Spring's own messaging/web method-invocation
 * infrastructure (e.g. [org.springframework.messaging.handler.invocation.InvocableHandlerMethod], which
 * `spring-kafka`'s own [org.springframework.kafka.listener.adapter.KotlinAwareInvocableHandlerMethod]
 * extends) uses to represent "a method plus the instance to invoke it
 * on," rather than the two as separate, uncorrelated parameters.
 * @property acknowledgment the listener's [Acknowledgment] argument, if declared (manual ack mode). `null`
 * otherwise.
 * @property consumer the listener's raw [Consumer] argument, if declared. `null` otherwise.
 * @property methodArgumentValues all resolved arguments the listener method is about to be invoked with, in
 * declaration order (the compiler-generated [kotlin.coroutines.Continuation] parameter is not included) -- named after
 * Spring's own `InvocableHandlerMethod.getMethodArgumentValues()`, which is what actually produces them.
 * Placed last, and least likely to be needed: [SingleRecord]/[BatchRecords]/[IndividualParameters]'s own
 * fields already cover what most hooks care about.
 */
public sealed class KafkaListenerInvocation {
    public abstract val handlerMethod: HandlerMethod
    public abstract val acknowledgment: Acknowledgment?
    public abstract val consumer: Consumer<*, *>?
    public abstract val methodArgumentValues: List<Any?>

    /**
     * A single-record listener: `fun listener(record: ConsumerRecord<K, V>)`, optionally alongside other
     * parameters (e.g. [Acknowledgment], [Consumer]).
     * @property record the listener's [ConsumerRecord] argument.
     */
    public class SingleRecord internal constructor(
        override val handlerMethod: HandlerMethod,
        override val acknowledgment: Acknowledgment?,
        override val consumer: Consumer<*, *>?,
        public val record: ConsumerRecord<*, *>,
        override val methodArgumentValues: List<Any?>,
    ) : KafkaListenerInvocation()

    /**
     * A batch listener: `fun listener(records: List<ConsumerRecord<K, V>>)`.
     * @property records the listener's batch of records.
     */
    public class BatchRecords internal constructor(
        override val handlerMethod: HandlerMethod,
        override val acknowledgment: Acknowledgment?,
        override val consumer: Consumer<*, *>?,
        public val records: List<ConsumerRecord<*, *>>,
        override val methodArgumentValues: List<Any?>,
    ) : KafkaListenerInvocation()

    /**
     * A listener with no [ConsumerRecord] (single or batched) parameter at all -- individual
     * `@Payload`/`@Header`-annotated parameters instead. Unlike [SingleRecord]/[BatchRecords], there's no
     * single fixed shape here: which `@Header`s (if any) a listener declares, and what type it declares
     * `@Payload` as, is chosen per listener method, not something this library can know in advance --
     * [payload]/[headers] are as far as that can be made properly named/typed; [methodArgumentValues]
     * remains the positional fallback.
     * @property payload the `@Payload`-annotated parameter's resolved value, if the method declares one.
     * `null` if it doesn't (including the case where a parameter is treated as the payload implicitly,
     * without an explicit `@Payload` annotation -- this only recognizes an explicit one).
     * @property headers `@Header`-annotated parameters, keyed by the header name each one declares (e.g.
     * [org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY]) rather than by declaration position.
     */
    public class IndividualParameters internal constructor(
        override val handlerMethod: HandlerMethod,
        override val acknowledgment: Acknowledgment?,
        override val consumer: Consumer<*, *>?,
        public val payload: Any?,
        public val headers: Map<String, Any?>,
        override val methodArgumentValues: List<Any?>,
    ) : KafkaListenerInvocation()
}

/**
 * Wraps a suspend `@KafkaListener` invocation, for [KafkaListenerCoroutineHookAspect] -- inspired by
 * Spring WebFlux's `CoWebFilter` (same abstract-class-plus-`@Component`-subclass shape: one abstract
 * suspend method, meant to be subclassed and registered as a Spring bean, rather than a
 * functional-interface lambda), but named `Hook`, not `Filter`: unlike `CoWebFilter.filter(exchange,
 * chain)`, this doesn't receive a mutable request/response-like value to inspect or replace before
 * handing it to the next link in a chain -- [KafkaListenerInvocation] is a read-only description of the
 * call already picked apart, and `processMessage` is the listener body itself, not "the next filter."
 * Rewriting the incoming record is [org.springframework.kafka.listener.adapter.RecordInterceptor]'s job,
 * not this one. What this actually does is wrap the call -- before/after/around, same as any hook -- so
 * `Hook` fits, `Filter` doesn't.
 *
 * You receive the listener's own resolved invocation details and a `processMessage` suspend function
 * representing the real listener body, and decide how (and whether) to call it. A trivial no-op
 * implementation just calls it straight through:
 *
 * ```kotlin
 * @Component
 * class NoOpHook : KafkaListenerCoroutineHook() {
 *     override suspend fun hook(invocation: KafkaListenerInvocation, processMessage: suspend () -> Any?): Any? =
 *         processMessage()
 * }
 * ```
 *
 * Since this is a real suspend function wrapping another suspend function, ordinary coroutine idioms
 * apply directly -- no separate "return a [kotlin.coroutines.CoroutineContext]" API is needed. To install a
 * [kotlin.coroutines.CoroutineContext.Element] (e.g. a correlation id extracted from the incoming
 * record's headers) for `processMessage`, and anything it suspends into, to read back:
 *
 * ```kotlin
 * @Component
 * class CorrelationIdHook : KafkaListenerCoroutineHook() {
 *     override suspend fun hook(invocation: KafkaListenerInvocation, processMessage: suspend () -> Any?): Any? {
 *         val correlationId = ... // extracted from invocation
 *         return withContext(CorrelationId(correlationId)) { processMessage() }
 *     }
 * }
 * ```
 *
 * To run the listener body on a specific dispatcher instead of `spring-kafka`'s own default
 * ([kotlinx.coroutines.Dispatchers.Unconfined]), [kotlinx.coroutines.withContext] works the same way:
 * `withContext(Dispatchers.IO) { processMessage() }`.
 *
 * Because the implementation decides whether to call `processMessage` at all, it can also do things a
 * "return extra context" API couldn't: short-circuit without invoking the listener, retry it, catch and
 * translate exceptions, or run code both before *and* after it completes. `processMessage`'s (and so
 * [hook]'s own) return type is deliberately `Any?`, not `Unit`: a suspend `@KafkaListener` method can have
 * a real, non-`Unit` return type for `@SendTo`-style reply production, and this Aspect intercepts every
 * suspend listener invocation, not just `Unit`-returning ones -- forcing `Unit` here would silently
 * discard that reply value for any listener run through a non-[NONE] hook.
 */
public abstract class KafkaListenerCoroutineHook {
    /**
     * @param invocation the listener method about to be invoked, its resolved arguments, and which of
     * [KafkaListenerInvocation]'s shapes it takes.
     * @param processMessage the real listener body. Call it (typically exactly once) to actually invoke
     * the listener; its result (or exception) becomes the result of this call.
     * @return the value to return to `spring-kafka` in place of the listener's own return value --
     * ordinarily just whatever `processMessage()` itself returned.
     */
    public abstract suspend fun hook(
        invocation: KafkaListenerInvocation,
        processMessage: suspend () -> Any?,
    ): Any?

    public companion object {
        /**
         * A [KafkaListenerCoroutineHook] that calls straight through, unchanged. Recognized by
         * [KafkaListenerCoroutineHookAspect] as a fast path: listeners are left on plain `proceed()`
         * without going through any of this Aspect's reflection-based machinery at all.
         */
        public val NONE: KafkaListenerCoroutineHook =
            object : KafkaListenerCoroutineHook() {
                override suspend fun hook(
                    invocation: KafkaListenerInvocation,
                    processMessage: suspend () -> Any?,
                ): Any? = processMessage()
            }
    }
}
