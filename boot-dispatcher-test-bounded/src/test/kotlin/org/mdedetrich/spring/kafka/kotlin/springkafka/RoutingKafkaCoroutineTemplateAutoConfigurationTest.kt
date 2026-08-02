package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.asCoroutineDispatcher
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.RoutingKafkaTemplate
import java.time.Duration
import java.util.concurrent.Executors
import java.util.regex.Pattern
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

// A minimal, broker-free ProducerFactory: hands out a caller-supplied MockProducer instead of creating a
// real Producer, mirroring RoutingMockProducerFactory in each consuming module's own
// RoutingKafkaCoroutineTemplateTest.
private class RoutingMockProducerFactory(
    private val producer: MockProducer<Any, Any>,
) : ProducerFactory<Any, Any> {
    override fun createProducer(): MockProducer<Any, Any> = producer
}

private fun routingStringSerializerAsAny(): Serializer<Any> = StringSerializer() as Serializer<Any>

private fun routingMockProducer(): MockProducer<Any, Any> =
    MockProducer(true, null, routingStringSerializerAsAny(), routingStringSerializerAsAny())

// A RoutingKafkaTemplate whose receive(...) doesn't touch a real Consumer/broker at all -- it just records
// which thread it ran on, mirroring ThreadRecordingKafkaTemplate in
// KafkaCoroutineTemplateAutoConfigurationTest.
private class ThreadRecordingRoutingKafkaTemplate(
    producerFactory: ProducerFactory<Any, Any>,
) : RoutingKafkaTemplate(mapOf(Pattern.compile("orders.*") to producerFactory)) {
    @Volatile
    var lastReceiveThreadName: String? = null

    override fun receive(
        topic: String,
        partition: Int,
        offset: Long,
        pollTimeout: Duration,
    ): ConsumerRecord<Any, Any> {
        lastReceiveThreadName = Thread.currentThread().name
        return ConsumerRecord(topic, partition, offset, "key-1", "payload")
    }
}

private suspend fun currentThreadName(): String = suspendCoroutine { continuation -> continuation.resume(Thread.currentThread().name) }

/**
 * Verifies [RoutingKafkaCoroutineTemplateAutoConfiguration] actually wires an autowirable
 * [RoutingKafkaCoroutineTemplate] rather than just unit-testing that class in isolation (see each
 * consuming module's own `RoutingKafkaCoroutineTemplateTest`).
 */
class RoutingKafkaCoroutineTemplateAutoConfigurationTest {
    private lateinit var context: AnnotationConfigApplicationContext

    @AfterEach
    fun tearDown() {
        context.close()
    }

    @Configuration
    private class RoutingKafkaTemplateConfig {
        @Bean
        fun routingKafkaTemplate(): RoutingKafkaTemplate =
            RoutingKafkaTemplate(mapOf(Pattern.compile("orders.*") to RoutingMockProducerFactory(routingMockProducer())))
    }

    @Configuration
    private class ThreadRecordingRoutingKafkaTemplateConfig {
        @Bean
        fun routingKafkaTemplate(): ThreadRecordingRoutingKafkaTemplate =
            ThreadRecordingRoutingKafkaTemplate(RoutingMockProducerFactory(routingMockProducer()))
    }

    @Configuration
    private class CustomRoutingKafkaCoroutineTemplateConfig {
        @Bean
        fun customRoutingKafkaCoroutineTemplate(routingKafkaTemplate: RoutingKafkaTemplate) =
            RoutingKafkaCoroutineTemplate(routingKafkaTemplate)
    }

    @Configuration
    private class QualifiedDispatcherConfig {
        @Bean
        @BlockingIODispatcher
        fun blockingIODispatcher() =
            Executors
                .newSingleThreadExecutor { runnable ->
                    Thread(runnable, "receive-test-thread")
                }.asCoroutineDispatcher()
    }

    // An unqualified CoroutineDispatcher bean, e.g. one an application defines for some unrelated feature
    // -- must NOT be picked up by routingKafkaCoroutineTemplate(), since the qualifier scopes the lookup.
    @Configuration
    private class UnqualifiedDispatcherConfig {
        @Bean
        fun someOtherDispatcher() =
            Executors
                .newSingleThreadExecutor { runnable ->
                    Thread(runnable, "unrelated-thread")
                }.asCoroutineDispatcher()
    }

    @Test
    fun `wires a RoutingKafkaCoroutineTemplate when a RoutingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(RoutingKafkaTemplateConfig::class.java, RoutingKafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertTrue(context.containsBean("routingKafkaCoroutineTemplate"))
        context.getBean(RoutingKafkaCoroutineTemplate::class.java)
    }

    @Test
    fun `does not wire a RoutingKafkaCoroutineTemplate when no RoutingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(RoutingKafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertFalse(context.containsBean("routingKafkaCoroutineTemplate"))
    }

    @Test
    fun `backs off when a RoutingKafkaCoroutineTemplate bean is already present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            RoutingKafkaTemplateConfig::class.java,
            CustomRoutingKafkaCoroutineTemplateConfig::class.java,
            RoutingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val custom = context.getBean("customRoutingKafkaCoroutineTemplate", RoutingKafkaCoroutineTemplate::class.java)
        val wired = context.getBean(RoutingKafkaCoroutineTemplate::class.java)

        assertSame(custom, wired)
        assertFalse(context.containsBean("routingKafkaCoroutineTemplate"))
    }

    @Test
    @Timeout(10)
    suspend fun `uses Dispatchers-IO by default when no qualified dispatcher bean is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingRoutingKafkaTemplateConfig::class.java,
            RoutingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingRoutingKafkaTemplate::class.java)
        val callingThreadName = currentThreadName()
        val coroutineTemplate = context.getBean(RoutingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.receive("orders", 0, 0L)

        assertFalse(delegate.lastReceiveThreadName == callingThreadName)
        assertTrue(delegate.lastReceiveThreadName?.contains("DefaultDispatcher") == true)
    }

    @Test
    @Timeout(10)
    suspend fun `uses the qualified CoroutineDispatcher bean when present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingRoutingKafkaTemplateConfig::class.java,
            UnqualifiedDispatcherConfig::class.java,
            QualifiedDispatcherConfig::class.java,
            RoutingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingRoutingKafkaTemplate::class.java)
        val coroutineTemplate = context.getBean(RoutingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.receive("orders", 0, 0L)

        assertEquals(true, delegate.lastReceiveThreadName?.startsWith("receive-test-thread"))
    }
}
