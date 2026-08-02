package org.mdedetrich.spring.kafka.kotlin.aop

import org.springframework.core.MethodParameter
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

// Must genuinely implement Continuation, not just be some unrelated placeholder object: whatever
// argument-resolution puts in this array slot still has to pass java.lang.reflect.Method.invoke's own
// runtime type-check against the parameter's declared (raw, erased) type before any interceptor chain --
// ours included -- ever runs. Its methods are still effectively inert in the real, proxied-bean scenario
// this library is for: KafkaListenerCoroutineHookAspect's advice fires *before* the underlying suspend
// method body ever executes (that's what being AOP-proxied means), invokes the real body itself via
// kotlin-reflect with its own independently-managed continuation, and never reads this one at all (see
// classifyInvocation, which discards args.last() entirely). resumeWith would only ever be invoked for
// real if this factory's InvocableHandlerMethod were invoked directly against an *unproxied* bean whose
// suspend method genuinely suspends -- not a scenario this library's own machinery creates.
private object ContinuationPlaceholder : Continuation<Any?> {
    override val context: CoroutineContext = EmptyCoroutineContext

    override fun resumeWith(result: Result<Any?>) = Unit
}

// spring-kafka's own suspend-@KafkaListener support (KotlinAwareInvocableHandlerMethod +
// ContinuationHandlerMethodArgumentResolver + KafkaMessageHandlerMethodFactory) is @since 3.2 -- see
// hook-aspect-native/README.md and hook-aspect-compat/README.md for the full mechanism. Before 3.2, the
// listener container's generic argument-resolution step has no resolver that recognizes a
// Continuation-typed parameter (the one the Kotlin compiler silently appends to every suspend function's
// real, compiled signature) and fails outright -- before the method is ever actually invoked, regardless
// of what this library does. This resolver is the fix for that specific step.
private class ContinuationPlaceholderArgumentResolver : HandlerMethodArgumentResolver {
    override fun supportsParameter(parameter: MethodParameter): Boolean = Continuation::class.java.isAssignableFrom(parameter.parameterType)

    override fun resolveArgument(
        parameter: MethodParameter,
        message: Message<*>,
    ): Any = ContinuationPlaceholder
}

/**
 * A [org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory] that backports
 * suspend `@KafkaListener` support to spring-kafka versions before 3.2, by adding a resolver for the
 * trailing [Continuation] parameter every compiled suspend function has -- the one piece spring-kafka's
 * own listener container needs to successfully invoke a suspend listener method at all. See
 * `hook-aspect-compat/README.md` for why this is necessary, and why it's sufficient on its own (no
 * custom [org.springframework.messaging.handler.invocation.InvocableHandlerMethod] override needed) once paired with
 * [KafkaListenerCoroutineHookAspect]'s own synchronous invocation design.
 *
 * Must be registered as a Spring bean visible to [org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor] for suspend
 * `@KafkaListener` methods to work at all on spring-kafka before 3.2 -- an explicit setup step 3.2+
 * doesn't require, since spring-kafka ships the equivalent ([org.springframework.kafka.listener.adapter.KafkaMessageHandlerMethodFactory]) itself
 * from that version on:
 *
 * ```kotlin
 * @Bean
 * fun messageHandlerMethodFactory(): MessageHandlerMethodFactory = KafkaListenerCoroutineMessageHandlerMethodFactory()
 * ```
 */
public class KafkaListenerCoroutineMessageHandlerMethodFactory : DefaultMessageHandlerMethodFactory() {
    override fun initArgumentResolvers(): MutableList<HandlerMethodArgumentResolver> {
        val resolvers = super.initArgumentResolvers()
        // Inserted one before the end, same placement spring-kafka's own resolver uses -- the last
        // resolver in the list is PayloadMethodArgumentResolver, which (per its own documentation) treats
        // any otherwise-unclaimed parameter as an implicit payload, so anything meant to claim a parameter
        // by type must run before it.
        resolvers.add(resolvers.size - 1, ContinuationPlaceholderArgumentResolver())
        return resolvers
    }
}
