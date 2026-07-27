package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
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
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate
import org.springframework.kafka.requestreply.CorrelationKey
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import kotlin.time.Duration.Companion.seconds

class AggregatingReplyingKafkaCoroutineTemplateTest {
    // No embedded broker in this module; both factories point at an unreachable port with a short
    // max.block.ms/request timeout so real (non-mock) calls fail fast instead of hanging.
    //
    // AggregatingReplyingKafkaTemplate's container is typed GenericMessageListenerContainer<K,
    // Collection<ConsumerRecord<K, R>>>, not <K, R> -- the consumer factory is only ever used
    // contravariantly (ConsumerFactory<? super K, ? super V>), so a plain ConsumerFactory<Any, Any>
    // satisfies it regardless; the container itself still needs the precise type arguments spelled
    // out explicitly since Kotlin won't infer them from an Any-typed factory.
    private fun unstartedTemplate(): AggregatingReplyingKafkaTemplate<String, String, String> {
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
            DefaultKafkaConsumerFactory<Any, Any>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.GROUP_ID_CONFIG to "aggregating-replying-kafka-coroutine-template-test",
                ),
            )
        // AggregatingReplyingKafkaTemplate manages its own offset commits as replies are aggregated
        // (checkOffsetsAndCommitIfNecessary), so it requires the container to use manual acking -- it
        // asserts this in its constructor.
        val containerProperties = ContainerProperties("replies").apply { ackMode = ContainerProperties.AckMode.MANUAL }
        val container =
            ConcurrentMessageListenerContainer<String, Collection<ConsumerRecord<String, String>>>(
                consumerFactory,
                containerProperties,
            )
        return AggregatingReplyingKafkaTemplate(producerFactory, container) { records, _ -> records.size >= 2 }
    }

    // For the success-path typed sendAndReceive test: a real, startable AggregatingReplyingKafkaTemplate
    // backed by a MockProducer (so the request "send" completes synchronously without a broker). The
    // reply container is never actually polled -- onMessage(...) (inherited, unmodified, from
    // ReplyingKafkaTemplate<K, V, Collection<ConsumerRecord<K, R>>>) is invoked directly to fabricate the
    // aggregated reply instead, so no embedded/reachable broker is needed.
    private fun mockProducerTemplate(): AggregatingReplyingKafkaTemplate<String, String, String> {
        val producerFactory =
            object : ProducerFactory<String, String> {
                override fun createProducer() = MockProducer(true, StringSerializer(), StringSerializer())
            }
        val consumerFactory =
            DefaultKafkaConsumerFactory<Any, Any>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                    ConsumerConfig.GROUP_ID_CONFIG to "aggregating-replying-kafka-coroutine-template-typed-test",
                ),
            )
        val containerProperties = ContainerProperties("replies").apply { ackMode = ContainerProperties.AckMode.MANUAL }
        val container =
            ConcurrentMessageListenerContainer<String, Collection<ConsumerRecord<String, String>>>(
                consumerFactory,
                containerProperties,
            )
        return AggregatingReplyingKafkaTemplate(producerFactory, container) { records, _ -> records.size >= 2 }
    }

    @Test
    fun `AggregatingReplyingKafkaCoroutineTemplate wraps the AggregatingReplyingKafkaTemplate delegate`() {
        val delegate = unstartedTemplate()
        val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

        assertSame(delegate, coroutineTemplate.delegate)
    }

    @Test
    fun `KafkaCoroutineOperations surface is reachable via interface delegation`() {
        val delegate = unstartedTemplate()
        val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

        // producerFactory and metrics are KafkaCoroutineOperations interface members, not declared
        // anywhere on AggregatingReplyingKafkaCoroutineTemplate itself -- reachable only through the
        // `by` delegation to the internal KafkaCoroutineTemplate(delegate).
        assertSame(delegate.producerFactory, coroutineTemplate.producerFactory)
        assertNotNull(coroutineTemplate.metrics)
    }

    @Test
    fun `configuration setters, including the two Aggregating-specific ones, proxy through without throwing`() {
        val delegate = unstartedTemplate()
        val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

        val scheduler = ThreadPoolTaskScheduler().apply { initialize() }
        coroutineTemplate.setTaskScheduler(scheduler)
        coroutineTemplate.setDefaultReplyTimeout(5.seconds)
        coroutineTemplate.setSharedReplyTopic(true)
        coroutineTemplate.setCorrelationHeaderName("customCorrelationId")
        coroutineTemplate.setReplyTopicHeaderName("customReplyTopic")
        coroutineTemplate.setReplyPartitionHeaderName("customReplyPartition")
        coroutineTemplate.setReplyErrorChecker { _ -> null }
        coroutineTemplate.setBinaryCorrelation(false)

        // The two members genuinely new to AggregatingReplyingKafkaTemplate itself.
        coroutineTemplate.setCommitTimeout(10.seconds)
        coroutineTemplate.setReturnPartialOnTimeout(true)

        assertEquals(0, coroutineTemplate.getAssignedReplyTopicPartitions().size)
    }

    @Test
    fun `KafkaCoroutineTemplate-only extras are hand-forwarded to the delegate`() {
        val delegate = unstartedTemplate()
        val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

        coroutineTemplate.defaultTopic = "orders"
        assertEquals("orders", delegate.defaultTopic)
        assertEquals("orders", coroutineTemplate.defaultTopic)

        val kafkaAdmin = KafkaAdmin(emptyMap())
        coroutineTemplate.setKafkaAdmin(kafkaAdmin)
        assertSame(kafkaAdmin, coroutineTemplate.kafkaAdmin)

        coroutineTemplate.setMicrometerTagsProvider { record -> mapOf("topic" to record.topic()) }
        val first = coroutineTemplate.getMicrometerTagsProvider()
        val second = coroutineTemplate.getMicrometerTagsProvider()
        assertNotNull(first)
        assertSame(first, second)
    }

    @Test
    @Timeout(10)
    suspend fun `sendAndReceive propagates failure when the reply container was never started`() {
        val delegate = unstartedTemplate()
        val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

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
    suspend fun `typed sendAndReceive completes successfully and future#get() returns a properly typed Message`() =
        coroutineScope {
            val delegate = mockProducerTemplate()
            val correlationId = CorrelationKey(byteArrayOf(1, 2, 3, 4))
            delegate.setCorrelationIdStrategy { correlationId }
            delegate.start()
            try {
                val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)
                val message =
                    MessageBuilder
                        .withPayload("payload")
                        .setHeader(KafkaHeaders.TOPIC, "requests")
                        .build()

                // Dispatchers.Unconfined runs the coroutine synchronously up to its first real suspension
                // point (the future.await() inside sendAndReceive), so by the time async(...) returns
                // control here, the request has already been "sent" (MockProducer auto-completes) and its
                // correlation id is already registered with the delegate -- safe to fabricate and deliver
                // the matching aggregated reply immediately afterwards, with no race.
                val deferred =
                    async(Dispatchers.Unconfined) {
                        coroutineTemplate.sendAndReceive(
                            message,
                            object : ParameterizedTypeReference<Collection<ConsumerRecord<String, String>>>() {},
                        )
                    }

                val aggregatedValue: MutableCollection<ConsumerRecord<String, String>> =
                    mutableListOf(ConsumerRecord("orders", 0, 0L, "key-2", "inner-payload"))
                val replyRecord: ConsumerRecord<String, MutableCollection<ConsumerRecord<String, String>>> =
                    ConsumerRecord("replies", 0, 0L, "key-1", aggregatedValue).apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                delegate.onMessage(mutableListOf(replyRecord))

                val result = deferred.await()

                assertEquals(aggregatedValue, result.payload)
            } finally {
                delegate.stop()
            }
        }

    @Test
    @Timeout(10)
    suspend fun `sendAndReceiveDeferred exposes send and reply as independent deferreds`() {
        val delegate = mockProducerTemplate()
        val correlationId = CorrelationKey(byteArrayOf(9, 9, 9, 9))
        delegate.setCorrelationIdStrategy { correlationId }
        delegate.start()
        try {
            val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)

            val result = coroutineTemplate.sendAndReceiveDeferred(ProducerRecord("requests", "key-1", "payload"))

            // MockProducer auto-completes synchronously, so the send half is already done by the time
            // sendAndReceiveDeferred returns -- awaitable independently of the reply, which hasn't
            // arrived yet.
            val sendResult = result.sendResult.await()
            assertEquals("requests", sendResult.recordMetadata.topic())

            val aggregatedValue: MutableCollection<ConsumerRecord<String, String>> =
                mutableListOf(ConsumerRecord("orders", 0, 0L, "key-2", "inner-payload"))
            val replyRecord: ConsumerRecord<String, MutableCollection<ConsumerRecord<String, String>>> =
                ConsumerRecord("replies", 0, 0L, "key-1", aggregatedValue).apply {
                    headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                }
            delegate.onMessage(mutableListOf(replyRecord))

            val reply = result.reply.await()
            assertEquals(aggregatedValue, reply.value())
        } finally {
            delegate.stop()
        }
    }

    @Test
    @Timeout(10)
    suspend fun `reified sendAndReceiveTyped avoids the manual ParameterizedTypeReference boilerplate`() =
        coroutineScope {
            val delegate = mockProducerTemplate()
            val correlationId = CorrelationKey(byteArrayOf(3, 1, 4, 1))
            delegate.setCorrelationIdStrategy { correlationId }
            delegate.start()
            try {
                val coroutineTemplate = AggregatingReplyingKafkaCoroutineTemplate(delegate)
                val message =
                    MessageBuilder
                        .withPayload("payload")
                        .setHeader(KafkaHeaders.TOPIC, "requests")
                        .build()

                // Kotlin doesn't allow specifying only *some* of a function's type arguments explicitly
                // (all four -- reified P, K, V, R -- or none), so P is inferred here from the expected
                // type of `result` instead of an explicit type argument at the call site.
                val deferred =
                    async(Dispatchers.Unconfined) {
                        val result: Message<Collection<ConsumerRecord<String, String>>> =
                            coroutineTemplate.sendAndReceiveTyped(message)
                        result
                    }

                val aggregatedValue: MutableCollection<ConsumerRecord<String, String>> =
                    mutableListOf(ConsumerRecord("orders", 0, 0L, "key-2", "inner-payload"))
                val replyRecord: ConsumerRecord<String, MutableCollection<ConsumerRecord<String, String>>> =
                    ConsumerRecord("replies", 0, 0L, "key-1", aggregatedValue).apply {
                        headers().add(RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.correlationId))
                    }
                delegate.onMessage(mutableListOf(replyRecord))

                val result = deferred.await()

                assertEquals(aggregatedValue, result.payload)
            } finally {
                delegate.stop()
            }
        }
}
