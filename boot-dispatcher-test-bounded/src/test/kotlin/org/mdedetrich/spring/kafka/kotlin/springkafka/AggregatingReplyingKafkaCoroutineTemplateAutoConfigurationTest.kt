package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.asCoroutineDispatcher
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringDeserializer
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.seconds

// The only genuinely abstract member of ProducerFactory (every other method has a default
// implementation) -- never actually invoked here, since these tests only verify bean wiring, not real
// message sending.
private class AggregatingFakeProducerFactory : ProducerFactory<String, String> {
    override fun createProducer(): Producer<String, String> = throw UnsupportedOperationException("not used in this test")
}

private fun aggregatingMockProducerFactory() =
    object : ProducerFactory<String, String> {
        override fun createProducer() = MockProducer(true, null, StringSerializer(), StringSerializer())
    }

// AggregatingReplyingKafkaTemplate manages its own offset commits as replies are aggregated, so it requires
// the container to use manual acking -- it asserts this in its constructor. autoStartup is disabled on both
// the container and the template itself so registering this as a Spring bean and refreshing the context
// never attempts a real connection -- mirrors `unstartedTemplate()` in each consuming module's own
// `AggregatingReplyingKafkaCoroutineTemplateTest`.
private fun unstartedAggregatingReplyContainer(): ConcurrentMessageListenerContainer<String, Collection<ConsumerRecord<String, String>>> {
    val consumerFactory =
        DefaultKafkaConsumerFactory<Any, Any>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.GROUP_ID_CONFIG to "aggregating-replying-kafka-coroutine-template-autoconfiguration-test",
            ),
        )
    val containerProperties = ContainerProperties("replies").apply { ackMode = ContainerProperties.AckMode.MANUAL }
    val container =
        ConcurrentMessageListenerContainer<String, Collection<ConsumerRecord<String, String>>>(
            consumerFactory,
            containerProperties,
        )
    container.isAutoStartup = false
    return container
}

// An AggregatingReplyingKafkaTemplate whose waitForAssignment(...) doesn't touch a real Consumer/broker at
// all -- it just records which thread it ran on, mirroring ThreadRecordingReplyingKafkaTemplate in
// ReplyingKafkaCoroutineTemplateAutoConfigurationTest.
private class ThreadRecordingAggregatingReplyingKafkaTemplate(
    producerFactory: ProducerFactory<String, String>,
    replyContainer: ConcurrentMessageListenerContainer<String, Collection<ConsumerRecord<String, String>>>,
) : AggregatingReplyingKafkaTemplate<String, String, String>(producerFactory, replyContainer, { records, _ -> records.size >= 2 }) {
    @Volatile
    var lastWaitForAssignmentThreadName: String? = null

    override fun waitForAssignment(duration: Duration): Boolean {
        lastWaitForAssignmentThreadName = Thread.currentThread().name
        return true
    }
}

private suspend fun currentThreadName(): String = suspendCoroutine { continuation -> continuation.resume(Thread.currentThread().name) }

/**
 * Verifies [AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration] actually wires an autowirable
 * [AggregatingReplyingKafkaCoroutineTemplate] rather than just unit-testing that class in isolation (see
 * each consuming module's own `AggregatingReplyingKafkaCoroutineTemplateTest`).
 */
class AggregatingReplyingKafkaCoroutineTemplateAutoConfigurationTest {
    private lateinit var context: AnnotationConfigApplicationContext

    @AfterEach
    fun tearDown() {
        context.close()
    }

    @Configuration
    private class AggregatingReplyingKafkaTemplateConfig {
        @Bean
        fun aggregatingReplyingKafkaTemplate(): AggregatingReplyingKafkaTemplate<String, String, String> =
            AggregatingReplyingKafkaTemplate(
                AggregatingFakeProducerFactory(),
                unstartedAggregatingReplyContainer(),
            ) { records, _ -> records.size >= 2 }.apply { isAutoStartup = false }
    }

    @Configuration
    private class ThreadRecordingAggregatingReplyingKafkaTemplateConfig {
        @Bean
        fun aggregatingReplyingKafkaTemplate(): ThreadRecordingAggregatingReplyingKafkaTemplate =
            ThreadRecordingAggregatingReplyingKafkaTemplate(aggregatingMockProducerFactory(), unstartedAggregatingReplyContainer())
                .apply { isAutoStartup = false }
    }

    @Configuration
    private class CustomAggregatingReplyingKafkaCoroutineTemplateConfig {
        @Bean
        fun customAggregatingReplyingKafkaCoroutineTemplate(
            aggregatingReplyingKafkaTemplate: AggregatingReplyingKafkaTemplate<String, String, String>,
        ) = AggregatingReplyingKafkaCoroutineTemplate(aggregatingReplyingKafkaTemplate)
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
    // -- must NOT be picked up by aggregatingReplyingKafkaCoroutineTemplate(), since the qualifier scopes
    // the lookup.
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
    fun `wires an AggregatingReplyingKafkaCoroutineTemplate when an AggregatingReplyingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            AggregatingReplyingKafkaTemplateConfig::class.java,
            AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        assertTrue(context.containsBean("aggregatingReplyingKafkaCoroutineTemplate"))
        context.getBean(AggregatingReplyingKafkaCoroutineTemplate::class.java)
    }

    @Test
    fun `does not wire an AggregatingReplyingKafkaCoroutineTemplate when no AggregatingReplyingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertFalse(context.containsBean("aggregatingReplyingKafkaCoroutineTemplate"))
    }

    @Test
    fun `backs off when an AggregatingReplyingKafkaCoroutineTemplate bean is already present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            AggregatingReplyingKafkaTemplateConfig::class.java,
            CustomAggregatingReplyingKafkaCoroutineTemplateConfig::class.java,
            AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val custom =
            context.getBean("customAggregatingReplyingKafkaCoroutineTemplate", AggregatingReplyingKafkaCoroutineTemplate::class.java)
        val wired = context.getBean(AggregatingReplyingKafkaCoroutineTemplate::class.java)

        assertSame(custom, wired)
        assertFalse(context.containsBean("aggregatingReplyingKafkaCoroutineTemplate"))
    }

    @Test
    @Timeout(10)
    suspend fun `uses Dispatchers-IO by default when no qualified dispatcher bean is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingAggregatingReplyingKafkaTemplateConfig::class.java,
            AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingAggregatingReplyingKafkaTemplate::class.java)
        val callingThreadName = currentThreadName()
        val coroutineTemplate = context.getBean(AggregatingReplyingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.waitForAssignment(1.seconds)

        assertFalse(delegate.lastWaitForAssignmentThreadName == callingThreadName)
        assertTrue(delegate.lastWaitForAssignmentThreadName?.contains("DefaultDispatcher") == true)
    }

    @Test
    @Timeout(10)
    suspend fun `uses the qualified CoroutineDispatcher bean when present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingAggregatingReplyingKafkaTemplateConfig::class.java,
            UnqualifiedDispatcherConfig::class.java,
            QualifiedDispatcherConfig::class.java,
            AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingAggregatingReplyingKafkaTemplate::class.java)
        val coroutineTemplate = context.getBean(AggregatingReplyingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.waitForAssignment(1.seconds)

        assertEquals(true, delegate.lastWaitForAssignmentThreadName?.startsWith("receive-test-thread"))
    }
}
