package org.mdedetrich.spring.kafka.kotlin.springkafka

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

// A minimal, broker-free ProducerFactory: hands out a caller-supplied MockProducer instead of
// opening a real connection, so KafkaTemplate can be exercised without an embedded broker.
private class MockProducerFactory(
    private val producer: MockProducer<String, String>,
) : ProducerFactory<String, String> {
    override fun createProducer(): Producer<String, String> = producer
}

// Mirrors how a Spring Boot application would actually wire this: KafkaTemplate is its own bean,
// KafkaCoroutineTemplate receives it via plain constructor injection. proxyBeanMethods = false
// (lite mode) avoids requiring CGLIB to subclass a final Kotlin class.
@Configuration(proxyBeanMethods = false)
class KafkaCoroutineTemplateTestConfig {
    @Bean
    fun producerFactory(): ProducerFactory<String, String> =
        MockProducerFactory(MockProducer(true, null, StringSerializer(), StringSerializer()))

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, String>): KafkaTemplate<String, String> = KafkaTemplate(producerFactory)

    @Bean
    fun kafkaCoroutineTemplate(kafkaTemplate: KafkaTemplate<String, String>): KafkaCoroutineTemplate<String, String> =
        KafkaCoroutineTemplate(kafkaTemplate)
}

// Documents a downstream usage pattern: an application's own @Configuration class receiving
// KafkaCoroutineTemplate via @Autowired field injection, exactly like any other Spring bean dependency.
@Configuration(proxyBeanMethods = false)
class OrderProducerConfig {
    @Autowired
    lateinit var kafkaTemplate: KafkaCoroutineTemplate<String, String>

    @Bean
    fun orderProducer(): OrderProducer = OrderProducer(kafkaTemplate)
}

class OrderProducer(
    val coroutineTemplate: KafkaCoroutineTemplate<String, String>,
)

// Documents the other downstream usage pattern: an application's own @Configuration class receiving
// KafkaCoroutineTemplate via plain constructor injection, exactly like any other Spring bean dependency.
@Configuration(proxyBeanMethods = false)
class OrderProducerConstructorConfig(
    private val coroutineTemplate: KafkaCoroutineTemplate<String, String>,
) {
    @Bean
    fun orderProducer(): OrderProducer = OrderProducer(coroutineTemplate)
}

class KafkaCoroutineTemplateTest {
    private fun testContext() = AnnotationConfigApplicationContext(KafkaCoroutineTemplateTestConfig::class.java)

    @Test
    fun `KafkaCoroutineTemplate wraps the KafkaTemplate bean via standard constructor injection`() {
        testContext().use { context ->
            val kafkaTemplate = context.getBean(KafkaTemplate::class.java)
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)

            assertSame(kafkaTemplate, coroutineTemplate.delegate)
        }
    }

    @Test
    fun `defaultTopic property proxies reads and writes through to the delegate`() {
        testContext().use { context ->
            val kafkaTemplate = context.getBean(KafkaTemplate::class.java)
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)

            coroutineTemplate.defaultTopic = "orders"

            assertEquals("orders", kafkaTemplate.defaultTopic)
            assertEquals("orders", coroutineTemplate.defaultTopic)
        }
    }

    @Test
    fun `producerFactory property proxies to the delegate's producer factory`() {
        testContext().use { context ->
            val producerFactory = context.getBean(ProducerFactory::class.java)
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)

            assertSame(producerFactory, coroutineTemplate.producerFactory)
        }
    }

    @Test
    fun `KafkaCoroutineTemplate can be Autowired into a downstream @Configuration class`() {
        AnnotationConfigApplicationContext(
            KafkaCoroutineTemplateTestConfig::class.java,
            OrderProducerConfig::class.java,
        ).use { context ->
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)
            val orderProducer = context.getBean(OrderProducer::class.java)

            assertSame(coroutineTemplate, orderProducer.coroutineTemplate)
        }
    }

    @Test
    fun `KafkaCoroutineTemplate can be injected into a downstream @Configuration class constructor`() {
        AnnotationConfigApplicationContext(
            KafkaCoroutineTemplateTestConfig::class.java,
            OrderProducerConstructorConfig::class.java,
        ).use { context ->
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)
            val orderProducer = context.getBean(OrderProducer::class.java)

            assertSame(coroutineTemplate, orderProducer.coroutineTemplate)
        }
    }

    @Test
    fun `getMicrometerTagsProvider caches the wrapped lambda until the delegate's live provider changes`() {
        val mockProducer = MockProducer(true, null, StringSerializer(), StringSerializer())
        val kafkaTemplate = KafkaTemplate(MockProducerFactory(mockProducer))
        val coroutineTemplate = KafkaCoroutineTemplate(kafkaTemplate)

        coroutineTemplate.setMicrometerTagsProvider { record -> mapOf("topic" to record.topic()) }

        val first = coroutineTemplate.getMicrometerTagsProvider()
        val second = coroutineTemplate.getMicrometerTagsProvider()

        assertNotNull(first)
        assertSame(first, second)

        val record = ProducerRecord("orders", "key-1", "payload")
        assertEquals(mapOf("topic" to "orders"), first!!(record))

        // Changing the provider directly on the delegate, bypassing this wrapper entirely, must still
        // invalidate the cache -- proves it tracks the delegate's live state, not just its own setter calls.
        kafkaTemplate.setMicrometerTagsProvider { _ -> mapOf("changed" to "true") }
        val third = coroutineTemplate.getMicrometerTagsProvider()

        assertNotNull(third)
        assertNotSame(first, third)
        assertEquals(mapOf("changed" to "true"), third!!(record))
    }

    @Test
    @Timeout(10)
    suspend fun `KafkaCoroutineTemplate works with a real DefaultKafkaProducerFactory, propagating send failures`() {
        // No embedded broker in this module; use an unreachable port with a short max.block.ms so a real
        // (non-mock) send fails fast instead of hanging, proving exceptions from the actual Kafka client
        // machinery propagate correctly through the suspend/.await() boundary, not just MockProducer's path.
        val producerFactory =
            DefaultKafkaProducerFactory<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG to "200",
                ),
            )
        val kafkaTemplate = KafkaTemplate(producerFactory)
        val coroutineTemplate = KafkaCoroutineTemplate(kafkaTemplate)

        assertSame(producerFactory, coroutineTemplate.producerFactory)

        var thrown: Throwable? = null
        try {
            coroutineTemplate.send("orders", "key-1", "payload")
        } catch (e: Throwable) {
            thrown = e
        }

        assertNotNull(thrown)

        producerFactory.destroy()
    }

    @Test
    suspend fun `send suspends and actually delegates through KafkaTemplate to the underlying Producer`() {
        val mockProducer = MockProducer(true, null, StringSerializer(), StringSerializer())
        val kafkaTemplate = KafkaTemplate(MockProducerFactory(mockProducer))
        val coroutineTemplate = KafkaCoroutineTemplate(kafkaTemplate)

        val result = coroutineTemplate.send("orders", "key-1", "payload")

        assertEquals("orders", result.recordMetadata.topic())

        val sent = mockProducer.history().single()
        assertEquals("orders", sent.topic())
        assertEquals("key-1", sent.key())
        assertEquals("payload", sent.value())
    }
}
