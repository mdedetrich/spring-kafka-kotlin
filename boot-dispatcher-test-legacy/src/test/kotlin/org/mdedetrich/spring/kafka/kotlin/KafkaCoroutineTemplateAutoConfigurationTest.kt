package org.mdedetrich.spring.kafka.kotlin

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
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
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

// The only genuinely abstract member of ProducerFactory (every other method has a default
// implementation) -- never actually invoked here, since these tests only verify bean wiring, not real
// message sending.
private class FakeProducerFactory : ProducerFactory<String, String> {
    override fun createProducer(): Producer<String, String> = throw UnsupportedOperationException("not used in this test")
}

private fun mockProducerFactory() =
    object : ProducerFactory<String, String> {
        override fun createProducer() = MockProducer(true, StringSerializer(), StringSerializer())
    }

// A KafkaTemplate whose receive(...) doesn't touch a real Consumer/broker at all -- it just records
// which thread it ran on, mirroring the same fixture each consuming module's own
// `KafkaCoroutineTemplateReceiveTest` uses to assert on dispatcher placement without needing a reachable
// broker.
private class ThreadRecordingKafkaTemplate(
    producerFactory: ProducerFactory<String, String>,
) : KafkaTemplate<String, String>(producerFactory) {
    @Volatile
    var lastReceiveThreadName: String? = null

    override fun receive(
        topic: String,
        partition: Int,
        offset: Long,
        pollTimeout: Duration,
    ): ConsumerRecord<String, String> {
        lastReceiveThreadName = Thread.currentThread().name
        return ConsumerRecord(topic, partition, offset, "key-1", "payload")
    }
}

private suspend fun currentThreadName(): String = suspendCoroutine { continuation -> continuation.resume(Thread.currentThread().name) }

/**
 * Verifies [KafkaCoroutineTemplateAutoConfiguration] actually wires an autowirable [KafkaCoroutineTemplate]
 * rather than just unit-testing that class in isolation (see each consuming module's own
 * `KafkaCoroutineTemplateTest`).
 */
class KafkaCoroutineTemplateAutoConfigurationTest {
    private lateinit var context: AnnotationConfigApplicationContext

    @AfterEach
    fun tearDown() {
        context.close()
    }

    @Configuration
    private class KafkaTemplateConfig {
        @Bean
        fun kafkaTemplate(): KafkaTemplate<String, String> = KafkaTemplate(FakeProducerFactory())
    }

    @Configuration
    private class ThreadRecordingKafkaTemplateConfig {
        @Bean
        fun kafkaTemplate(): ThreadRecordingKafkaTemplate = ThreadRecordingKafkaTemplate(mockProducerFactory())
    }

    @Configuration
    private class CustomKafkaCoroutineTemplateConfig {
        @Bean
        fun customKafkaCoroutineTemplate(kafkaTemplate: KafkaTemplate<String, String>) = KafkaCoroutineTemplate(kafkaTemplate)
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
    // -- must NOT be picked up by kafkaCoroutineTemplate(), since the qualifier scopes the lookup.
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
    fun `wires a KafkaCoroutineTemplate when a KafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(KafkaTemplateConfig::class.java, KafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertTrue(context.containsBean("kafkaCoroutineTemplate"))
        context.getBean(KafkaCoroutineTemplate::class.java)
    }

    @Test
    fun `does not wire a KafkaCoroutineTemplate when no KafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(KafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertFalse(context.containsBean("kafkaCoroutineTemplate"))
    }

    @Test
    fun `backs off when a KafkaCoroutineTemplate bean is already present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            KafkaTemplateConfig::class.java,
            CustomKafkaCoroutineTemplateConfig::class.java,
            KafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val custom = context.getBean("customKafkaCoroutineTemplate", KafkaCoroutineTemplate::class.java)
        val wired = context.getBean(KafkaCoroutineTemplate::class.java)

        assertSame(custom, wired)
        assertFalse(context.containsBean("kafkaCoroutineTemplate"))
    }

    // Plain fun + runBlocking, not `suspend fun`: JUnit 5.14.4 (spring-kafka-kotlin-2.8-boot/2.9-boot's
    // JVM 8 floor) doesn't execute suspend @Test methods -- see KafkaCoroutineTemplateReceiveTest's
    // identical note in spring-kafka-kotlin-2.8/2.9.
    @Test
    @Timeout(10)
    fun `uses Dispatchers-IO by default when no qualified dispatcher bean is present`() =
        runBlocking {
            context = AnnotationConfigApplicationContext()
            context.register(ThreadRecordingKafkaTemplateConfig::class.java, KafkaCoroutineTemplateAutoConfiguration::class.java)
            context.refresh()

            val delegate = context.getBean(ThreadRecordingKafkaTemplate::class.java)
            val callingThreadName = currentThreadName()
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)

            coroutineTemplate.receive("orders", 0, 0L)

            assertFalse(delegate.lastReceiveThreadName == callingThreadName)
            assertTrue(delegate.lastReceiveThreadName?.contains("DefaultDispatcher") == true)
        }

    @Test
    @Timeout(10)
    fun `uses the qualified CoroutineDispatcher bean when present`() =
        runBlocking {
            context = AnnotationConfigApplicationContext()
            context.register(
                ThreadRecordingKafkaTemplateConfig::class.java,
                UnqualifiedDispatcherConfig::class.java,
                QualifiedDispatcherConfig::class.java,
                KafkaCoroutineTemplateAutoConfiguration::class.java,
            )
            context.refresh()

            val delegate = context.getBean(ThreadRecordingKafkaTemplate::class.java)
            val coroutineTemplate = context.getBean(KafkaCoroutineTemplate::class.java)

            coroutineTemplate.receive("orders", 0, 0L)

            assertEquals(true, delegate.lastReceiveThreadName?.startsWith("receive-test-thread"))
        }
}
