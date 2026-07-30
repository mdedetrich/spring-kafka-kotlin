package org.mdedetrich.spring.kafka.kotlin.springkafka.aop

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

// The motivating use case for KafkaListenerCoroutineHook: propagate a correlation id, read from the
// incoming ConsumerRecord's headers, to the listener body (and anything it suspends into) via a
// CoroutineContext.Element -- the @KafkaListener analogue of what CoWebFilter does for WebFlux handlers.
data class CorrelationId(
    val value: String,
) : AbstractCoroutineContextElement(CorrelationId) {
    companion object Key : CoroutineContext.Key<CorrelationId>
}

// kotlinx.coroutines debug mode (enabled by default when assertions are on, e.g. under Gradle's test
// JVM) renames each coroutine's thread with a "@coroutine#N" suffix -- since the Aspect relaunches the
// listener body in a genuinely new coroutine, that suffix differs from the caller's even when both run
// on the exact same underlying thread. Compare only the real thread name, ignoring that suffix.
private fun currentBaseThreadName(): String = Thread.currentThread().name.substringBefore(" @coroutine")

private fun correlationIdHeader(record: ConsumerRecord<*, *>): String? =
    record
        .headers()
        .lastHeader("correlationId")
        ?.value()
        ?.let { String(it, StandardCharsets.UTF_8) }

// KafkaListenerCoroutineHook is an abstract class (mirroring CoWebFilter's own shape), meant to be
// subclassed and registered as a Spring bean -- this local helper just keeps the many small, throwaway
// hooks in this test file from each needing a named `object : ... () { override ... }`.
private fun hook(block: suspend (KafkaListenerInvocation, suspend () -> Any?) -> Any?): KafkaListenerCoroutineHook =
    object : KafkaListenerCoroutineHook() {
        override suspend fun hook(
            invocation: KafkaListenerInvocation,
            processMessage: suspend () -> Any?,
        ): Any? = block(invocation, processMessage)
    }

// Exhaustive `when` over the sealed KafkaListenerInvocation -- adding a new case to the sealed class
// would be a compile error here until handled, rather than a silently-ignored shape. This is a real
// suspend function wrapping processMessage, CoWebFilter-style: no correlation id found just calls
// processMessage() straight through; a correlation id installs it via withContext first.
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

// No real Kafka broker or listener container anywhere in this test -- @EnableKafka is deliberately never
// applied, so @KafkaListener is inert metadata here, matched only by this project's own Aspect (an
// ordinary AspectJ @annotation pointcut, independent of spring-kafka's own listener machinery). The
// listener bean is called directly, like any other proxied Spring bean method.
open class RecordingListener {
    @KafkaListener(topics = ["orders"], groupId = "test", autoStartup = "false")
    open suspend fun processMessage(record: ConsumerRecord<String, String>): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        val threadName = currentBaseThreadName()
        delay(10)
        return "processed:${record.value()}:correlationId=$correlationId:thread=$threadName"
    }

    @KafkaListener(topics = ["failures"], groupId = "test", autoStartup = "false")
    open suspend fun processAndFail(record: ConsumerRecord<String, String>): String = throw IllegalStateException("boom: ${record.value()}")

    // Batch listener -- classified as KafkaListenerInvocation.BatchRecords from the method's *declared*
    // List<ConsumerRecord<K, V>> parameter type, not by guessing from the (possibly empty) runtime list.
    @KafkaListener(topics = ["orders-batch"], groupId = "test", autoStartup = "false")
    open suspend fun processBatch(records: List<ConsumerRecord<String, String>>): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        return "processed:${records.joinToString(",") { it.value() }}:correlationId=$correlationId"
    }

    // Individual @Payload parameter, no ConsumerRecord at all -- classified as
    // KafkaListenerInvocation.IndividualParameters.
    @KafkaListener(topics = ["orders-payload"], groupId = "test", autoStartup = "false")
    open suspend fun processPayload(
        @Payload value: String,
    ): String {
        val correlationId = coroutineContext[CorrelationId]?.value
        return "processed:$value:correlationId=$correlationId"
    }

    // @Payload + @Header individual parameters -- proves KafkaListenerInvocation.IndividualParameters'
    // payload/headers fields are populated from the method's actual parameter annotations, keyed by
    // header name rather than declaration position.
    @KafkaListener(topics = ["orders-headers"], groupId = "test", autoStartup = "false")
    open suspend fun processWithHeaders(
        @Payload value: String,
        @Header("correlationId") correlationId: String?,
        @Header("customHeader") custom: String,
    ): String {
        val propagatedCorrelationId = coroutineContext[CorrelationId]?.value
        return "processed:$value:custom=$custom:correlationId=$propagatedCorrelationId"
    }

    // Acknowledgment/Consumer alongside a single-record listener -- both should end up on
    // KafkaListenerInvocation.SingleRecord (and equally on BatchRecords/IndividualParameters, since
    // they're valid alongside any of the record shapes, not just this one).
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

// The pattern this whole feature is designed for: a real, top-level class (not an anonymous `object :`
// expression) extending KafkaListenerCoroutineHook and registered via @Component, exactly the way
// CoWebFilter subclasses are registered in a real Spring application.
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
    @Timeout(10)
    suspend fun `correlation id extracted from record headers is visible inside the listener body`() {
        val callingThreadName = currentBaseThreadName()

        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-1", "abc-123")

            val result = listener.processMessage(record)

            assertEquals("processed:order-1:correlationId=abc-123:thread=$callingThreadName", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `no correlation id header means processMessage is called straight through`() {
        val callingThreadName = currentBaseThreadName()

        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-2", correlationId = null)

            val result = listener.processMessage(record)

            // No correlationId header -> the hook calls processMessage() directly, with no
            // withContext -- still launched via the Aspect (on Dispatchers.Unconfined, matching
            // spring-kafka's own default), so the same underlying thread as an un-intercepted call.
            assertEquals("processed:order-2:correlationId=null:thread=$callingThreadName", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `a contributed dispatcher relocates the listener while correlation id is still propagated`() {
        val dispatcherThreadCounter = AtomicInteger()
        val dispatcherThread =
            Executors
                .newSingleThreadExecutor { r -> Thread(r, "listener-dispatcher-${dispatcherThreadCounter.incrementAndGet()}") }
                .asCoroutineDispatcher()
        // Composition, CoWebFilter-style: nest one hook's wrapping inside another's.
        val dispatcherHook =
            hook { invocation, processMessage ->
                withContext(dispatcherThread) { correlationIdHook.hook(invocation, processMessage) }
            }

        contextWithHook(dispatcherHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-3", "xyz-789")

            val result = listener.processMessage(record)

            assertTrue(result.startsWith("processed:order-3:correlationId=xyz-789:thread=listener-dispatcher-"))
        }
    }

    @Test
    @Timeout(10)
    suspend fun `exception from the listener body propagates to the caller, not swallowed`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("bad", "abc-123")

            val ex = assertThrows<IllegalStateException> { listener.processAndFail(record) }

            assertEquals("boom: bad", ex.message)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `handlerMethod bundles the real target bean and method, not the AOP proxy`() {
        var capturedHandlerMethod: org.springframework.messaging.handler.HandlerMethod? = null
        val handlerMethodCapturingHook =
            hook { invocation, processMessage ->
                capturedHandlerMethod = invocation.handlerMethod
                processMessage()
            }

        contextWithHook(handlerMethodCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-11", correlationId = null)

            listener.processMessage(record)

            val handlerMethod = capturedHandlerMethod
            assertTrue(handlerMethod != null)
            assertEquals("processMessage", handlerMethod!!.method.name)
            // The real target, not the CGLIB proxy: RecordingListener's own class, not a $$SpringCGLIB$$ subclass.
            assertEquals(RecordingListener::class.java, handlerMethod.bean.javaClass)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `Acknowledgment and Consumer parameters are extracted onto the invocation regardless of shape`() {
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

            listener.processWithAck(record, acknowledgment, mockConsumer)

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
    @Timeout(10)
    suspend fun `batch listener is classified as BatchRecords, correlation id read from its first record`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val records = listOf(recordWithCorrelationId("order-6", "batch-123"), recordWithCorrelationId("order-7", null))

            val result = listener.processBatch(records)

            assertEquals("processed:order-6,order-7:correlationId=batch-123", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `an empty batch is still classified as BatchRecords, not guessed from the empty runtime list`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            // No records to read a correlation id from -> processMessage() called straight through.
            // The point: classification must come from the declared List<ConsumerRecord<K, V>> parameter
            // type, since an empty list carries no runtime type information to guess from.
            val result = listener.processBatch(emptyList())

            assertEquals("processed::correlationId=null", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `individual @Payload parameter listener is classified as IndividualParameters`() {
        contextWithHook(correlationIdHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            // No ConsumerRecord argument at all for the hook to read a header from -> processMessage()
            // called straight through.
            val result = listener.processPayload("order-8")

            assertEquals("processed:order-8:correlationId=null", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `IndividualParameters payload and headers are keyed by name, from the method's actual annotations`() {
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

            val result = listener.processWithHeaders("order-10", "corr-999", "custom-value")

            assertEquals("processed:order-10:custom=custom-value:correlationId=corr-999", result)
            assertEquals("order-10", capturedPayload)
            assertEquals(mapOf("correlationId" to "corr-999", "customHeader" to "custom-value"), capturedHeaders)
        }
    }

    // methodArgumentValues is backed by a zero-copy Array.asList() view rather than an eager copy (see
    // KafkaListenerCoroutineHookAspect.classifyInvocation) -- verifies that's indistinguishable from a
    // real List, both in content and in structural equality, from the hook's perspective.
    @Test
    @Timeout(10)
    suspend fun `methodArgumentValues reflects all resolved arguments, in declaration order`() {
        var captured: List<Any?>? = null
        val methodArgumentValuesCapturingHook =
            hook { invocation, processMessage ->
                captured = invocation.methodArgumentValues
                processMessage()
            }

        contextWithHook(methodArgumentValuesCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)

            listener.processWithHeaders("order-16", "corr-1", "custom-1")

            assertEquals(listOf("order-16", "corr-1", "custom-1"), captured)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `a real @Component subclass is discovered and used, matching CoWebFilter's own registration pattern`() {
        AnnotationConfigApplicationContext(ComponentScanConfig::class.java).use { context ->
            // Confirms component-scanning genuinely found and registered the subclass (not manually
            // wired via registerBean/@Bean, unlike every other test in this file).
            context.getBean(StaticCorrelationIdHook::class.java)

            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-12", correlationId = null)

            val result = listener.processMessage(record)

            assertTrue(result.startsWith("processed:order-12:correlationId=static-id:thread="))
        }
    }

    @Test
    @Timeout(10)
    suspend fun `KafkaListenerCoroutineHook NONE steps the Aspect aside entirely`() {
        val callingThreadName = currentBaseThreadName()

        contextWithHook(KafkaListenerCoroutineHook.NONE).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-5", "abc-123")

            val result = listener.processMessage(record)

            assertEquals("processed:order-5:correlationId=null:thread=$callingThreadName", result)
        }
    }

    // The relaunched coroutine that runs hook/processMessage builds on the hijacked continuation's own
    // CoroutineContext (see KafkaListenerCoroutineHookAspect), not just the Aspect's own bare scope -- so
    // a CoroutineContext.Element the caller already had installed is still visible inside the hook.
    data class CallerElement(
        val value: String,
    ) : AbstractCoroutineContextElement(CallerElement) {
        companion object Key : CoroutineContext.Key<CallerElement>
    }

    @Test
    @Timeout(10)
    suspend fun `the caller's own CoroutineContext element is visible inside the hook`() {
        var seenInsideHook: String? = null
        val contextCapturingHook =
            hook { _, processMessage ->
                seenInsideHook = coroutineContext[CallerElement]?.value
                processMessage()
            }

        contextWithHook(contextCapturingHook).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-14", correlationId = null)

            withContext(CallerElement("caller-side")) {
                listener.processMessage(record)
            }
        }

        assertEquals("caller-side", seenInsideHook)
    }

    // KafkaListenerCoroutineHookAspect starts each invocation as a child of its own aspect-owned Job
    // (via kotlin.coroutines.startCoroutine, not launch -- see the class/scope docs) specifically so
    // destroy() can still cancel invocations genuinely in flight when the Spring context shuts down.
    // Verifies that guarantee end-to-end: a hook stuck suspended in awaitCancellation() actually receives
    // a real CancellationException once the owning context is closed, and the original caller sees it too.
    @Test
    @Timeout(10)
    suspend fun `destroy cancels invocations still in flight`() {
        val started = CompletableDeferred<Unit>()
        var hookSawCancellation = false
        val hangingHook =
            hook { _, _ ->
                started.complete(Unit)
                try {
                    awaitCancellation()
                } catch (e: CancellationException) {
                    hookSawCancellation = true
                    throw e
                }
            }

        val context = contextWithHook(hangingHook)
        val listener = context.getBean(RecordingListener::class.java)
        val record = recordWithCorrelationId("order-15", correlationId = null)

        var callerSawCancellation = false
        val invocation =
            CoroutineScope(Dispatchers.Unconfined).launch {
                try {
                    listener.processMessage(record)
                } catch (e: CancellationException) {
                    callerSawCancellation = true
                }
            }

        started.await()
        context.close()
        invocation.join()

        assertTrue(hookSawCancellation)
        assertTrue(callerSawCancellation)
    }

    @Test
    @Timeout(10)
    suspend fun `hooks compose outermost-first, wrapping in list order like a filter chain`() {
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

            listener.processMessage(record)

            assertEquals(listOf("outer-before", "inner-before", "inner-after", "outer-after"), events)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `an empty hooks list steps the Aspect aside entirely, same as NONE`() {
        val callingThreadName = currentBaseThreadName()

        contextWithHooks(emptyList()).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-hooks-2", "abc-123")

            val result = listener.processMessage(record)

            assertEquals("processed:order-hooks-2:correlationId=null:thread=$callingThreadName", result)
        }
    }

    @Test
    @Timeout(10)
    suspend fun `a singleton hooks list behaves the same as the single-hook constructor`() {
        contextWithHooks(listOf(correlationIdHook)).use { context ->
            val listener = context.getBean(RecordingListener::class.java)
            val record = recordWithCorrelationId("order-hooks-3", "abc-123")

            val result = listener.processMessage(record)

            assertTrue(result.startsWith("processed:order-hooks-3:correlationId=abc-123:thread="))
        }
    }
}
