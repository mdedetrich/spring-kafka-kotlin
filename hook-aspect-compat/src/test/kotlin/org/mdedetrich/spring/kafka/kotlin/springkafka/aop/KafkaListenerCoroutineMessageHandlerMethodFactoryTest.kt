package org.mdedetrich.spring.kafka.kotlin.springkafka.aop

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.springframework.messaging.converter.MessageConversionException
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory
import org.springframework.messaging.support.MessageBuilder

// No @KafkaListener/spring-kafka container involved here at all -- this verifies the one specific,
// narrow thing KafkaListenerCoroutineMessageHandlerMethodFactory fixes: spring-messaging's own generic
// argument-resolution step, which every spring-kafka version (pre- and post-3.2) relies on.
class SuspendTarget {
    suspend fun echo(
        @Payload value: String,
    ): String = value.uppercase()
}

class KafkaListenerCoroutineMessageHandlerMethodFactoryTest {
    private val method =
        SuspendTarget::class.java.declaredMethods.first { it.name == "echo" }

    @Test
    @Timeout(10)
    fun `plain DefaultMessageHandlerMethodFactory cannot invoke a suspend method -- confirms the problem is real`() {
        val factory = DefaultMessageHandlerMethodFactory()
        factory.afterPropertiesSet()
        val handlerMethod = factory.createInvocableHandlerMethod(SuspendTarget(), method)
        val message = MessageBuilder.withPayload("hello").build()

        // No resolver in the default chain recognizes the compiler-generated trailing Continuation
        // parameter -- argument resolution itself fails, before the method is ever invoked. Confirmed
        // directly: PayloadMethodArgumentResolver ends up claiming it as an implicit, unclaimed payload
        // (since it's the last resolver in the chain) and fails trying to convert the incoming message
        // payload to a Continuation, throwing MessageConversionException -- not the generic
        // "no resolver found" error one might otherwise assume.
        assertThrows<MessageConversionException> { handlerMethod.invoke(message) }
    }

    @Test
    @Timeout(10)
    fun `KafkaListenerCoroutineMessageHandlerMethodFactory successfully invokes a suspend method`() {
        val factory = KafkaListenerCoroutineMessageHandlerMethodFactory()
        factory.afterPropertiesSet()
        val handlerMethod = factory.createInvocableHandlerMethod(SuspendTarget(), method)
        val message = MessageBuilder.withPayload("hello").build()

        // A real, synchronous invocation succeeding here is the whole point: argument resolution no
        // longer fails on the Continuation parameter, and the underlying (non-suspend-aware, pre-3.2)
        // InvocableHandlerMethod.doInvoke's plain Method.invoke(bean, args) still works correctly, since
        // KafkaListenerCoroutineHookAspect (when present) handles the actual suspend semantics itself --
        // this test exercises the un-proxied bean directly, so plain reflection is all that's needed.
        val result = handlerMethod.invoke(message)

        assertEquals("HELLO", result)
    }
}
