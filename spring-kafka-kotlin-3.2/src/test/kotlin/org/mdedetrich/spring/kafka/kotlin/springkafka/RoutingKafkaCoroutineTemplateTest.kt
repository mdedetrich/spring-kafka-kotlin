package org.mdedetrich.spring.kafka.kotlin.springkafka

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertThrows
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.RoutingKafkaTemplate
import java.util.regex.Pattern

class RoutingKafkaCoroutineTemplateTest {
    // A minimal, broker-free ProducerFactory: hands out a caller-supplied MockProducer instead of
    // opening a real connection, so RoutingKafkaTemplate can be exercised without an embedded broker.
    private class RoutingMockProducerFactory(
        private val producer: MockProducer<Any, Any>,
    ) : ProducerFactory<Any, Any> {
        override fun createProducer(): Producer<Any, Any> = producer
    }

    // StringSerializer implements Serializer<String>, not Serializer<Any> -- Java generics are
    // invariant, so it isn't directly assignable here even though String is-a Any. StringSerializer's
    // actual implementation doesn't use its generic parameter at runtime (just toString().toByteArray()),
    // so the cast is safe for a String-payload test even though RoutingKafkaTemplate is Any-typed.
    @Suppress("UNCHECKED_CAST")
    private fun stringSerializerAsAny(): Serializer<Any> = StringSerializer() as Serializer<Any>

    private fun mockProducer(): MockProducer<Any, Any> = MockProducer(true, stringSerializerAsAny(), stringSerializerAsAny())

    private fun routingKafkaTemplate(producerFactory: ProducerFactory<Any, Any>): RoutingKafkaTemplate =
        RoutingKafkaTemplate(mapOf(Pattern.compile("orders.*") to producerFactory))

    @Test
    fun `RoutingKafkaCoroutineTemplate wraps the RoutingKafkaTemplate delegate`() {
        val producerFactory = RoutingMockProducerFactory(mockProducer())
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        assertSame(delegate, coroutineTemplate.delegate)
    }

    @Test
    fun `getProducerFactory(topic) routes to the matching factory`() {
        val producerFactory = RoutingMockProducerFactory(mockProducer())
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        assertSame(producerFactory, coroutineTemplate.getProducerFactory("orders-created"))
    }

    @Test
    fun `getProducerFactory(topic) throws when no pattern matches`() {
        val producerFactory = RoutingMockProducerFactory(mockProducer())
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        assertThrows<IllegalStateException> {
            coroutineTemplate.getProducerFactory("unmatched-topic")
        }
    }

    @Test
    fun `disabled KafkaOperations members propagate UnsupportedOperationException through the interface delegation`() {
        val producerFactory = RoutingMockProducerFactory(mockProducer())
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        assertThrows<UnsupportedOperationException> { coroutineTemplate.flush() }
        assertThrows<UnsupportedOperationException> { coroutineTemplate.metrics }
    }

    @Test
    fun `KafkaCoroutineTemplate-only extras are hand-forwarded to the delegate`() {
        val producerFactory = RoutingMockProducerFactory(mockProducer())
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        coroutineTemplate.defaultTopic = "orders-created"
        assertEquals("orders-created", delegate.defaultTopic)
        assertEquals("orders-created", coroutineTemplate.defaultTopic)

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
    suspend fun `send suspends and routes to the matching factory's Producer`() {
        val mockProducer = mockProducer()
        val producerFactory = RoutingMockProducerFactory(mockProducer)
        val delegate = routingKafkaTemplate(producerFactory)
        val coroutineTemplate = RoutingKafkaCoroutineTemplate(delegate)

        val result = coroutineTemplate.send("orders-created", "key-1", "payload")

        assertEquals("orders-created", result.recordMetadata.topic())

        val sent = mockProducer.history().single()
        assertEquals("orders-created", sent.topic())
        assertEquals("key-1", sent.key())
        assertEquals("payload", sent.value())
    }
}
