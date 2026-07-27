package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertNotNull
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.requestreply.CorrelationKey
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.requestreply.RequestReplyFuture
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import kotlin.time.Duration.Companion.seconds

// For the cancellation test: captures the real RequestReplyFuture returned by the delegate's own
// sendAndReceive so the test can inspect its isCancelled state directly, without needing reflection into
// ReplyingKafkaTemplate's private `futures` map.
private class FutureCapturingReplyingKafkaTemplate(
    producerFactory: ProducerFactory<String, String>,
    replyContainer: ConcurrentMessageListenerContainer<String, String>,
) : ReplyingKafkaTemplate<String, String, String>(producerFactory, replyContainer) {
    lateinit var capturedFuture: RequestReplyFuture<String, String, String>

    override fun sendAndReceive(
        record: ProducerRecord<String, String>,
        replyTimeout: java.time.Duration?,
    ): RequestReplyFuture<String, String, String> {
        val future = super.sendAndReceive(record, replyTimeout)
        capturedFuture = future
        return future
    }
}

class ReplyingKafkaCoroutineTemplateTest {
    // No embedded broker in this module; both factories point at an unreachable port with a short
    // max.block.ms/request timeout so real (non-mock) calls fail fast instead of hanging.
    private fun unstartedReplyingKafkaTemplate(): ReplyingKafkaTemplate<String, String, String> {
        val producerFactory =
            DefaultKafkaProducerFactory<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG to "200",
                ),
            )
        val consumerFactory =
            DefaultKafkaConsumerFactory<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.GROUP_ID_CONFIG to "replying-kafka-coroutine-template-test",
                ),
            )
        val container = ConcurrentMessageListenerContainer(consumerFactory, ContainerProperties("replies"))
        return ReplyingKafkaTemplate(producerFactory, container)
    }

    // For the success-path typed sendAndReceive tests: a real, startable ReplyingKafkaTemplate backed by
    // a MockProducer (so the request "send" completes synchronously without a broker) and a reply
    // container that's never actually polled -- onMessage(...) is invoked directly to fabricate the
    // reply instead, so no embedded/reachable broker is needed to exercise the real completion path.
    private fun mockProducerReplyingKafkaTemplate(): ReplyingKafkaTemplate<String, String, String> {
        val producerFactory =
            object : ProducerFactory<String, String> {
                override fun createProducer() = MockProducer(true, StringSerializer(), StringSerializer())
            }
        val consumerFactory =
            DefaultKafkaConsumerFactory<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.GROUP_ID_CONFIG to "replying-kafka-coroutine-template-typed-test",
                ),
            )
        val container = ConcurrentMessageListenerContainer(consumerFactory, ContainerProperties("replies"))
        return ReplyingKafkaTemplate(producerFactory, container)
    }

    @Test
    fun `ReplyingKafkaCoroutineTemplate wraps the ReplyingKafkaTemplate delegate`() {
        val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
        val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

        assertSame(replyingKafkaTemplate, coroutineTemplate.delegate)
    }

    @Test
    fun `KafkaCoroutineOperations surface is reachable via interface delegation`() {
        val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
        val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

        // producerFactory and metrics are KafkaCoroutineOperations interface members, not declared
        // anywhere on ReplyingKafkaCoroutineTemplate itself -- reachable only through the `by` delegation.
        assertSame(replyingKafkaTemplate.producerFactory, coroutineTemplate.producerFactory)
        assertNotNull(coroutineTemplate.metrics)
    }

    @Test
    fun `KafkaCoroutineTemplate-only extras are hand-forwarded to the delegate`() {
        val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
        val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

        coroutineTemplate.defaultTopic = "orders"
        assertEquals("orders", replyingKafkaTemplate.defaultTopic)
        assertEquals("orders", coroutineTemplate.defaultTopic)

        coroutineTemplate.setMicrometerTagsProvider { record -> mapOf("topic" to record.topic()) }
        val first = coroutineTemplate.getMicrometerTagsProvider()
        val second = coroutineTemplate.getMicrometerTagsProvider()
        assertNotNull(first)
        assertSame(first, second)
    }

    @Test
    fun `configuration setters proxy through to the delegate without throwing`() {
        val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
        val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

        val scheduler = ThreadPoolTaskScheduler().apply { initialize() }
        coroutineTemplate.setTaskScheduler(scheduler)
        coroutineTemplate.setDefaultReplyTimeout(5.seconds)
        coroutineTemplate.setSharedReplyTopic(true)
        coroutineTemplate.setCorrelationIdStrategy { record -> CorrelationKey(record.topic().toByteArray()) }
        coroutineTemplate.setCorrelationHeaderName("customCorrelationId")
        coroutineTemplate.setReplyTopicHeaderName("customReplyTopic")
        coroutineTemplate.setReplyPartitionHeaderName("customReplyPartition")
        coroutineTemplate.setReplyErrorChecker { _: ConsumerRecord<*, *> -> null }

        assertEquals(0, coroutineTemplate.getAssignedReplyTopicPartitions().size)
    }

    // Plain fun + runBlocking, not `suspend fun`: JUnit 5.14.4 (this module's JVM 8 floor) doesn't
    // execute suspend @Test methods at all -- it logs a "must not return a value" warning and silently
    // skips them, no failure reported. JUnit 6.1.2 (3.0+) does support them natively.
    @Test
    @Timeout(10)
    fun `sendAndReceive propagates failure when the reply container was never started`() =
        runBlocking {
            val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
            val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

            var thrown: Throwable? = null
            try {
                coroutineTemplate.sendAndReceive(ProducerRecord("requests", "key-1", "payload"))
            } catch (e: Throwable) {
                thrown = e
            }

            assertNotNull(thrown)
        }

    @Test
    @Timeout(10)
    fun `typed sendAndReceive propagates failure when the reply container was never started`() =
        runBlocking {
            val replyingKafkaTemplate = unstartedReplyingKafkaTemplate()
            val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)
            val message =
                MessageBuilder
                    .withPayload("payload")
                    .setHeader(KafkaHeaders.TOPIC, "requests")
                    .build()

            var thrown: Throwable? = null
            try {
                coroutineTemplate.sendAndReceive(message, object : ParameterizedTypeReference<String>() {})
            } catch (e: Throwable) {
                thrown = e
            }

            assertNotNull(thrown)
        }

    @Test
    @Timeout(10)
    fun `typed sendAndReceive completes successfully and future#get() returns a properly typed Message`() =
        runBlocking {
            val replyingKafkaTemplate = mockProducerReplyingKafkaTemplate()
            val correlationId = CorrelationKey(byteArrayOf(1, 2, 3, 4))
            replyingKafkaTemplate.setCorrelationIdStrategy { correlationId }
            replyingKafkaTemplate.start()
            try {
                val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)
                val message =
                    MessageBuilder
                        .withPayload("payload")
                        .setHeader(KafkaHeaders.TOPIC, "requests")
                        .build()

                // Dispatchers.Unconfined runs the coroutine synchronously up to its first real suspension
                // point (the future.await() inside sendAndReceive), so by the time async(...) returns
                // control here, the request has already been "sent" (MockProducer auto-completes) and its
                // correlation id is already registered with the delegate -- safe to fabricate and deliver
                // the matching reply immediately afterwards, with no race.
                val deferred =
                    async(Dispatchers.Unconfined) {
                        coroutineTemplate.sendAndReceive(message, object : ParameterizedTypeReference<String>() {})
                    }

                val replyRecord =
                    ConsumerRecord("replies", 0, 0L, "key-1", "reply-payload").apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                replyingKafkaTemplate.onMessage(listOf(replyRecord))

                val result = deferred.await()

                assertEquals("reply-payload", result.payload)
            } finally {
                replyingKafkaTemplate.stop()
            }
        }

    @Test
    @Timeout(10)
    fun `typed sendAndReceive with an explicit reply timeout also returns a properly typed Message`() =
        runBlocking {
            val replyingKafkaTemplate = mockProducerReplyingKafkaTemplate()
            val correlationId = CorrelationKey(byteArrayOf(5, 6, 7, 8))
            replyingKafkaTemplate.setCorrelationIdStrategy { correlationId }
            replyingKafkaTemplate.start()
            try {
                val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)
                val message =
                    MessageBuilder
                        .withPayload("payload")
                        .setHeader(KafkaHeaders.TOPIC, "requests")
                        .build()

                val deferred =
                    async(Dispatchers.Unconfined) {
                        coroutineTemplate.sendAndReceive(
                            message,
                            10.seconds,
                            object : ParameterizedTypeReference<String>() {},
                        )
                    }

                val replyRecord =
                    ConsumerRecord("replies", 0, 0L, "key-1", "reply-payload").apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                replyingKafkaTemplate.onMessage(listOf(replyRecord))

                val result = deferred.await()

                assertEquals("reply-payload", result.payload)
            } finally {
                replyingKafkaTemplate.stop()
            }
        }

    @Test
    @Timeout(10)
    fun `sendAndReceiveDeferred exposes send and reply as independent deferreds`() =
        runBlocking {
            val replyingKafkaTemplate = mockProducerReplyingKafkaTemplate()
            val correlationId = CorrelationKey(byteArrayOf(9, 9, 9, 9))
            replyingKafkaTemplate.setCorrelationIdStrategy { correlationId }
            replyingKafkaTemplate.start()
            try {
                val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

                val result = coroutineTemplate.sendAndReceiveDeferred(ProducerRecord("requests", "key-1", "payload"))

                // MockProducer auto-completes synchronously, so the send half is already done by the time
                // sendAndReceiveDeferred returns -- awaitable independently of the reply, which hasn't
                // arrived yet.
                val sendResult = result.sendResult.await()
                assertEquals("requests", sendResult.recordMetadata.topic())

                val replyRecord =
                    ConsumerRecord("replies", 0, 0L, "key-1", "reply-payload").apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                replyingKafkaTemplate.onMessage(listOf(replyRecord))

                val reply = result.reply.await()
                assertEquals("reply-payload", reply.value())
            } finally {
                replyingKafkaTemplate.stop()
            }
        }

    @Test
    @Timeout(10)
    fun `reified sendAndReceive avoids the manual ParameterizedTypeReference boilerplate`() =
        runBlocking {
            val replyingKafkaTemplate = mockProducerReplyingKafkaTemplate()
            val correlationId = CorrelationKey(byteArrayOf(2, 4, 6, 8))
            replyingKafkaTemplate.setCorrelationIdStrategy { correlationId }
            replyingKafkaTemplate.start()
            try {
                val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)
                val message =
                    MessageBuilder
                        .withPayload("payload")
                        .setHeader(KafkaHeaders.TOPIC, "requests")
                        .build()

                // Kotlin doesn't allow specifying only *some* of a function's type arguments explicitly
                // (all four -- reified P, K, V, R -- or none), so P is inferred here from the expected
                // type of `result` instead of an explicit `<String>` at the call site.
                val deferred =
                    async(Dispatchers.Unconfined) {
                        val result: Message<String> = coroutineTemplate.sendAndReceiveTyped(message)
                        result
                    }

                val replyRecord =
                    ConsumerRecord("replies", 0, 0L, "key-1", "reply-payload").apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                replyingKafkaTemplate.onMessage(listOf(replyRecord))

                val result = deferred.await()

                assertEquals("reply-payload", result.payload)
            } finally {
                replyingKafkaTemplate.stop()
            }
        }

    @Test
    @Timeout(10)
    fun `cancelling the reply Deferred from sendAndReceiveDeferred cancels the underlying delegate future`() {
        val producerFactory =
            object : ProducerFactory<String, String> {
                override fun createProducer() = MockProducer(true, StringSerializer(), StringSerializer())
            }
        val consumerFactory =
            DefaultKafkaConsumerFactory<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.GROUP_ID_CONFIG to "replying-kafka-coroutine-template-cancel-test",
                ),
            )
        val container = ConcurrentMessageListenerContainer(consumerFactory, ContainerProperties("replies"))
        val replyingKafkaTemplate = FutureCapturingReplyingKafkaTemplate(producerFactory, container)
        replyingKafkaTemplate.start()
        try {
            val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

            val result = coroutineTemplate.sendAndReceiveDeferred(ProducerRecord("requests", "key-1", "payload"))
            result.reply.cancel()

            assertEquals(true, replyingKafkaTemplate.capturedFuture.isCancelled)
        } finally {
            replyingKafkaTemplate.stop()
        }
    }

    @Test
    @Timeout(10)
    fun `sendAndReceiveDeferred surfaces a send failure on sendResult independently of reply`() =
        runBlocking {
            // MockProducer with autoComplete = false: send() buffers the record without completing (or
            // throwing) synchronously, and errorNext(...) below fails it asynchronously via the producer
            // callback -- the same mechanism a real broker rejection would use post-buffering, unlike a
            // metadata-fetch timeout (which, verified separately, throws synchronously from sendAndReceive
            // itself rather than failing sendResult later). This is the scenario sendAndReceiveDeferred
            // exists for: observing the send failure on `sendResult` without it being entangled with `reply`,
            // which is still free to keep waiting for a correlated reply that may yet legitimately arrive.
            val mockProducer = MockProducer(false, StringSerializer(), StringSerializer())
            val producerFactory =
                object : ProducerFactory<String, String> {
                    override fun createProducer() = mockProducer
                }
            val consumerFactory =
                DefaultKafkaConsumerFactory<String, String>(
                    mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                        ConsumerConfig.GROUP_ID_CONFIG to "replying-kafka-coroutine-template-send-failure-test",
                    ),
                )
            val container = ConcurrentMessageListenerContainer(consumerFactory, ContainerProperties("replies"))
            val replyingKafkaTemplate = ReplyingKafkaTemplate(producerFactory, container)
            replyingKafkaTemplate.start()
            try {
                val coroutineTemplate = ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)

                val result = coroutineTemplate.sendAndReceiveDeferred(ProducerRecord("requests", "key-1", "payload"))
                mockProducer.errorNext(RuntimeException("broker rejected the send"))

                var thrown: Throwable? = null
                try {
                    result.sendResult.await()
                } catch (e: Throwable) {
                    thrown = e
                }
                assertNotNull(thrown)

                // The send failure must not have reached across to reply -- it's a separate future, still
                // free to keep waiting for a correlated reply that may yet legitimately arrive.
                assertEquals(false, result.reply.isCompleted)

                result.reply.cancel()
            } finally {
                replyingKafkaTemplate.stop()
            }
        }
}
