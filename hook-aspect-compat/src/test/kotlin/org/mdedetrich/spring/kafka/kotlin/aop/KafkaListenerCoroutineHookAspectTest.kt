package org.mdedetrich.spring.kafka.kotlin.aop

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.EnableAspectJAutoProxy
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

// Same motivating use case as hook-aspect-native's own test suite: propagate a correlation id read from the
// incoming ConsumerRecord's headers to the listener body (and anything it suspends into).
data class CorrelationId(
    val value: String,
) : AbstractCoroutineContextElement(CorrelationId) {
    companion object Key : CoroutineContext.Key<CorrelationId>
}

// kotlinx.coroutines debug mode renames each coroutine's thread with a "@coroutine#N" suffix. Compare
// only the real thread name, ignoring that suffix.
private fun currentBaseThreadName(): String = Thread.currentThread().name.substringBefore(" @coroutine")

private fun correlationIdHeader(record: ConsumerRecord<*, *>): String? =
    record
        .headers()
        .lastHeader("correlationId")
        ?.value()
        ?.let { String(it, StandardCharsets.UTF_8) }

private fun hook(block: suspend (KafkaListenerInvocation, suspend () -> Any?) -> Any?): KafkaListenerCoroutineHook =
    object : KafkaListenerCoroutineHook() {
        override suspend fun hook(
            invocation: KafkaListenerInvocation,
            processMessage: suspend () -> Any?,
        ): Any? = block(invocation, processMessage)
    }

private val correlationIdHook =
    hook { invocation, processMessage ->
        val correlationId =
            when (invocation) {
                is KafkaListenerInvocation.SingleRecord -> correlationIdHeader(invocation.record)
                is KafkaListenerInvocation.BatchRecords -> invocation.records.firstOrNull()?.let(::correlationIdHeader)
                is KafkaListenerInvocation.IndividualParameters -> invocation.headers["correlationId"] as? String
            }
        if (correlationId != null) withContext(CorrelationId(correlationId)) { processMessage() } else processMessage()
    }

// No real Kafka broker or listener container here -- @EnableKafka is deliberately never applied, so
// @KafkaListener is inert metadata, matched only by this Aspect's own AspectJ pointcut. Called directly,
// like any other proxied Spring bean method -- unlike production usage (where spring-kafka's container
// reflectively invokes via KafkaListenerCoroutineMessageHandlerMethodFactory), these tests call listener
// methods directly via real, compiled Kotlin suspend call sites.
open class RecordingListener {
    @KafkaListener(topics = ["orders"], groupId = "test", autoStartup = "false")
    open suspend fun processMessage(record: ConsumerRecord<String, String>): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        val threadName = currentBaseThreadName()
        return "processed:${record.value()}:correlationId=$correlationId:thread=$threadName"
    }

    @KafkaListener(topics = ["failures"], groupId = "test", autoStartup = "false")
    open suspend fun processAndFail(record: ConsumerRecord<String, String>): String = throw IllegalStateException("boom: ${record.value()}")

    @KafkaListener(topics = ["orders-batch"], groupId = "test", autoStartup = "false")
    open suspend fun processBatch(records: List<ConsumerRecord<String, String>>): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        return "processed:${records.joinToString(",") { it.value() }}:correlationId=$correlationId"
    }

    @KafkaListener(topics = ["orders-payload"], groupId = "test", autoStartup = "false")
    open suspend fun processPayload(
        @Payload value: String,
    ): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        return "processed:$value:correlationId=$correlationId"
    }

    @KafkaListener(topics = ["orders-headers"], groupId = "test", autoStartup = "false")
    open suspend fun processWithHeaders(
        @Payload value: String,
        @Header("correlationId") correlationId: String?,
        @Header("customHeader") custom: String,
    ): String {
        val propagatedCorrelationId = coroutineContext[CorrelationId]?.value
        return "processed:$value:custom=$custom:correlationId=$propagatedCorrelationId"
    }

    @KafkaListener(topics = ["orders-ack"], groupId = "test", autoStartup = "false")
    open suspend fun processWithAck(
        record: ConsumerRecord<String, String>,
        ack: Acknowledgment,
        consumer: Consumer<String, String>,
    ): String = "processed:${record.value()}"

    // Deliberately not suspend -- proves the Aspect leaves ordinary listeners alone.
    open fun processBlocking(record: ConsumerRecord<String, String>): String = "blocking:${record.value()}"
}

@Configuration
@EnableAspectJAutoProxy
open class RecordingListenerConfig(
    private val hook: KafkaListenerCoroutineHook,
) {
    @Bean
    open fun recordingListener() = RecordingListener()

    @Bean
    open fun kafkaListenerCoroutineHookAspect() = KafkaListenerCoroutineHookAspect(hook)
}

@Component
class StaticCorrelationIdHook : KafkaListenerCoroutineHook() {
    override suspend fun hook(
        invocation: KafkaListenerInvocation,
        processMessage: suspend () -> Any?,
    ): Any? = withContext(CorrelationId("static-id")) { processMessage() }
}

@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackageClasses = [StaticCorrelationIdHook::class])
open class ComponentScanConfig {
    @Bean
    open fun recordingListener() = RecordingListener()

    @Bean
    open fun kafkaListenerCoroutineHookAspect(hook: KafkaListenerCoroutineHook) = KafkaListenerCoroutineHookAspect(hook)
}

// Only registers recordingListener() -- the KafkaListenerCoroutineHookAspect(hooks) bean itself is
// registered directly via contextWithHooks below, bypassing Spring's own List<KafkaListenerCoroutineHook>
// autowiring (which sorts by @Order/Ordered) since these tests are about verifying the compose() ordering
// the constructor itself does, given an already-ordered list, not Spring's separate sorting guarantee.
@Configuration
@EnableAspectJAutoProxy
open class RecordingListenerOnlyConfig {
    @Bean
    open fun recordingListener() = RecordingListener()
}

class KafkaListenerCoroutineHookAspectTest {
    private fun contextWithHook(hook: KafkaListenerCoroutineHook) =
        AnnotationConfigApplicationContext().apply {
            registerBean(KafkaListenerCoroutineHook::class.java, java.util.function.Supplier { hook })
            register(RecordingListenerConfig::class.java)
            refresh()
        }

    private fun contextWithHooks(hooks: List<KafkaListenerCoroutineHook>) =
        AnnotationConfigApplicationContext().apply {
            registerBean(
                KafkaListenerCoroutineHookAspect::class.java,
                java.util.function.Supplier { KafkaListenerCoroutineHookAspect(hooks) },
            )
            register(RecordingListenerOnlyConfig::class.java)
            refresh()
        }

    private fun recordWithCorrelationId(
        value: String,
        correlationId: String?,
    ): ConsumerRecord<String, String> =
        ConsumerRecord("orders", 0, 0L, "key-1", value).apply {
            if (correlationId != null) {
                headers().add(RecordHeader("correlationId", correlationId.toByteArray(StandardCharsets.UTF_8)))
            }
        }

    @Test
    fun `correlation id extracted from record headers is visible inside the listener body`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-1", "abc-123")

            val result =
                kotlinx.coroutines.runBlocking {
                    listener.processMessage(record)
                }

            assertTrue(result.startsWith("processed:order-1:correlationId=abc-123:thread="))
        }
    }

    @Test
    fun `no correlation id header means processMessage is called straight through`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-2", correlationId = null)

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-2:correlationId=null:thread="))
        }
    }

    // The critical claim this backport's whole design rests on: aroundKafkaListener uses plain
    // runBlocking, not runBlocking(Dispatchers.Unconfined) -- so even though the hook relocates
    // processMessage onto a different single-thread dispatcher internally, the overall call still
    // returns control to (resumes) the *original calling thread*, exactly like an ordinary synchronous
    // method call would. Dispatchers.Unconfined would not guarantee this.
    @Test
    @Timeout(10)
    fun `the overall call returns on the calling thread even though the hook relocates processMessage internally`() {
        val callingThreadName = currentBaseThreadName()
        val dispatcherThread =
            Executors
                .newSingleThreadExecutor { r -> Thread(r, "listener-dispatcher") }
                .asCoroutineDispatcher()
        var threadInsideHook: String? = null
        val relocatingHook =
            hook { _, processMessage ->
                withContext(dispatcherThread) {
                    threadInsideHook = currentBaseThreadName()
                    processMessage()
                }
            }

        contextWithHook(relocatingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-3", correlationId = null)

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertEquals("listener-dispatcher", threadInsideHook)
            assertTrue(result.contains("thread=listener-dispatcher"))
            assertEquals(callingThreadName, currentBaseThreadName())
        }
    }

    // Guards against a real risk in replacing the current runBlocking-based bridge with something
    // lighter (see IMPLEMENTATION.md): runBlocking's own EventLoop lets nested same-thread continuations
    // (anything using Dispatchers.Unconfined inside hook/processMessage) trampoline through it while the
    // calling thread is blocked. A hook explicitly nesting Dispatchers.Unconfined is a realistic thing a
    // consumer might write (e.g. "run processMessage directly, no relocation") -- confirms it still
    // completes correctly and the overall call still returns on the calling thread.
    @Test
    @Timeout(10)
    fun `hook using nested Dispatchers Unconfined still completes correctly and returns on the calling thread`() {
        val callingThreadName = currentBaseThreadName()
        val unconfinedHook =
            hook { _, processMessage ->
                withContext(kotlinx.coroutines.Dispatchers.Unconfined) { processMessage() }
            }

        contextWithHook(unconfinedHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-13", correlationId = null)

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-13:correlationId=null:thread=$callingThreadName"))
            assertEquals(callingThreadName, currentBaseThreadName())
        }
    }

    @Test
    fun `exception from the listener body propagates to the caller, not swallowed`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("bad", "abc-123")

            val ex =
                assertThrows<IllegalStateException> {
                    kotlinx.coroutines.runBlocking { listener.processAndFail(record) }
                }

            assertEquals("boom: bad", ex.message)
        }
    }

    @Test
    fun `handlerMethod bundles the real target bean and method, not the AOP proxy`() {
        var capturedHandlerMethod: org.springframework.messaging.handler.HandlerMethod? = null
        val handlerMethodCapturingHook =
            hook { invocation, processMessage ->
                capturedHandlerMethod = invocation.handlerMethod
                processMessage()
            }

        contextWithHook(handlerMethodCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-11", correlationId = null)

            kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            val handlerMethod = capturedHandlerMethod
            assertTrue(handlerMethod != null)
            assertEquals("processMessage", handlerMethod!!.method.name)
            assertEquals(RecordingListener::class.java, handlerMethod.bean.javaClass)
        }
    }

    @Test
    fun `Acknowledgment and Consumer parameters are extracted onto the invocation regardless of shape`() {
        var capturedAcknowledgment: Acknowledgment? = null
        var capturedConsumer: Consumer<*, *>? = null
        val ackConsumerCapturingHook =
            hook { invocation, processMessage ->
                capturedAcknowledgment = invocation.acknowledgment
                capturedConsumer = invocation.consumer
                processMessage()
            }
        val acknowledgment =
            object : Acknowledgment {
                override fun acknowledge() = Unit
            }
        val mockConsumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)

        contextWithHook(ackConsumerCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-9", correlationId = null)

            kotlinx.coroutines.runBlocking { listener.processWithAck(record, acknowledgment, mockConsumer) }

            assertSame(acknowledgment, capturedAcknowledgment)
            assertSame(mockConsumer, capturedConsumer)
        }
    }

    @Test
    fun `non-suspend listener methods are left completely untouched`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-4", "abc-123")

            val result = listener.processBlocking(record)

            assertEquals("blocking:order-4", result)
        }
    }

    @Test
    fun `batch listener is classified as BatchRecords, correlation id read from its first record`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val records = listOf(recordWithCorrelationId("order-6", "batch-123"), recordWithCorrelationId("order-7", null))

            val result = kotlinx.coroutines.runBlocking { listener.processBatch(records) }

            assertEquals("processed:order-6,order-7:correlationId=batch-123", result)
        }
    }

    @Test
    fun `an empty batch is still classified as BatchRecords, not guessed from the empty runtime list`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            val result = kotlinx.coroutines.runBlocking { listener.processBatch(emptyList()) }

            assertEquals("processed::correlationId=null", result)
        }
    }

    @Test
    fun `individual @Payload parameter listener is classified as IndividualParameters`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            val result = kotlinx.coroutines.runBlocking { listener.processPayload("order-8") }

            assertEquals("processed:order-8:correlationId=null", result)
        }
    }

    @Test
    fun `IndividualParameters payload and headers are keyed by name, from the method's actual annotations`() {
        var capturedPayload: Any? = null
        var capturedHeaders: Map<String, Any?>? = null
        val payloadHeadersCapturingHook =
            hook { invocation, processMessage ->
                check(invocation is KafkaListenerInvocation.IndividualParameters)
                capturedPayload = invocation.payload
                capturedHeaders = invocation.headers
                val correlationId = invocation.headers["correlationId"] as? String
                if (correlationId != null) withContext(CorrelationId(correlationId)) { processMessage() } else processMessage()
            }

        contextWithHook(payloadHeadersCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            val result = kotlinx.coroutines.runBlocking { listener.processWithHeaders("order-10", "corr-999", "custom-value") }

            assertEquals("processed:order-10:custom=custom-value:correlationId=corr-999", result)
            assertEquals("order-10", capturedPayload)
            assertEquals(mapOf("correlationId" to "corr-999", "customHeader" to "custom-value"), capturedHeaders)
        }
    }

    @Test
    fun `a real @Component subclass is discovered and used, matching hook-aspect-3_2+'s own registration pattern`() {
        AnnotationConfigApplicationContext(ComponentScanConfig::class.java).use { context ->
            context.getBean(StaticCorrelationIdHook::class.java)

            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-12", correlationId = null)

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-12:correlationId=static-id:thread="))
        }
    }

    @Test
    fun `KafkaListenerCoroutineHook NONE steps the Aspect aside entirely`() {
        contextWithHook(KafkaListenerCoroutineHook.NONE).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-5", "abc-123")

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-5:correlationId=null:thread="))
        }
    }

    @Test
    fun `hooks compose outermost-first, wrapping in list order like a filter chain`() {
        val events = mutableListOf<String>()

        fun taggedHook(tag: String) =
            hook { _, processMessage ->
                events += "$tag-before"
                val result = processMessage()
                events += "$tag-after"
                result
            }

        contextWithHooks(listOf(taggedHook("outer"), taggedHook("inner"))).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-hooks-1", correlationId = null)

            kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertEquals(listOf("outer-before", "inner-before", "inner-after", "outer-after"), events)
        }
    }

    @Test
    fun `an empty hooks list steps the Aspect aside entirely, same as NONE`() {
        contextWithHooks(emptyList()).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-hooks-2", "abc-123")

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-hooks-2:correlationId=null:thread="))
        }
    }

    @Test
    fun `a singleton hooks list behaves the same as the single-hook constructor`() {
        contextWithHooks(listOf(correlationIdHook)).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-hooks-3", "abc-123")

            val result = kotlinx.coroutines.runBlocking { listener.processMessage(record) }

            assertTrue(result.startsWith("processed:order-hooks-3:correlationId=abc-123:thread="))
        }
    }
}
