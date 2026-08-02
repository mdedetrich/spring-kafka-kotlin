package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.asCoroutineDispatcher
import org.apache.kafka.clients.consumer.ConsumerConfig
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
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.seconds

// The only genuinely abstract member of ProducerFactory (every other method has a default
// implementation) -- never actually invoked here, since these tests only verify bean wiring, not real
// message sending.
private class ReplyingFakeProducerFactory : ProducerFactory<String, String> {
    override fun createProducer(): Producer<String, String> = throw UnsupportedOperationException("not used in this test")
}

private fun replyingMockProducerFactory() =
    object : ProducerFactory<String, String> {
        override fun createProducer() = MockProducer(true, null, StringSerializer(), StringSerializer())
    }

// No embedded broker in this module; autoStartup is disabled on both the container and the template
// itself so registering this as a Spring bean and refreshing the context never attempts a real connection
// -- mirrors `unstartedReplyingKafkaTemplate()` in each consuming module's own
// `ReplyingKafkaCoroutineTemplateTest`.
private fun unstartedReplyContainer(): ConcurrentMessageListenerContainer<String, String> {
    val consumerFactory =
        DefaultKafkaConsumerFactory<String, String>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.GROUP_ID_CONFIG to "replying-kafka-coroutine-template-autoconfiguration-test",
            ),
        )
    val container = ConcurrentMessageListenerContainer(consumerFactory, ContainerProperties("replies"))
    container.isAutoStartup = false
    return container
}

// A ReplyingKafkaTemplate whose waitForAssignment(...) doesn't touch a real Consumer/broker at all -- it
// just records which thread it ran on, mirroring ThreadRecordingKafkaTemplate in
// KafkaCoroutineTemplateAutoConfigurationTest.
private class ThreadRecordingReplyingKafkaTemplate(
    producerFactory: ProducerFactory<String, String>,
    replyContainer: ConcurrentMessageListenerContainer<String, String>,
) : ReplyingKafkaTemplate<String, String, String>(producerFactory, replyContainer) {
    @Volatile
    var lastWaitForAssignmentThreadName: String? = null

    override fun waitForAssignment(duration: Duration): Boolean {
        lastWaitForAssignmentThreadName = Thread.currentThread().name
        return true
    }
}

private suspend fun currentThreadName(): String = suspendCoroutine { continuation -> continuation.resume(Thread.currentThread().name) }

/**
 * Verifies [ReplyingKafkaCoroutineTemplateAutoConfiguration] actually wires an autowirable
 * [ReplyingKafkaCoroutineTemplate] rather than just unit-testing that class in isolation (see each
 * consuming module's own `ReplyingKafkaCoroutineTemplateTest`).
 */
class ReplyingKafkaCoroutineTemplateAutoConfigurationTest {
    private lateinit var context: AnnotationConfigApplicationContext

    @AfterEach
    fun tearDown() {
        context.close()
    }

    @Configuration
    private class ReplyingKafkaTemplateConfig {
        @Bean
        fun replyingKafkaTemplate(): ReplyingKafkaTemplate<String, String, String> =
            ReplyingKafkaTemplate(ReplyingFakeProducerFactory(), unstartedReplyContainer()).apply { isAutoStartup = false }
    }

    @Configuration
    private class ThreadRecordingReplyingKafkaTemplateConfig {
        @Bean
        fun replyingKafkaTemplate(): ThreadRecordingReplyingKafkaTemplate =
            ThreadRecordingReplyingKafkaTemplate(replyingMockProducerFactory(), unstartedReplyContainer()).apply { isAutoStartup = false }
    }

    @Configuration
    private class CustomReplyingKafkaCoroutineTemplateConfig {
        @Bean
        fun customReplyingKafkaCoroutineTemplate(replyingKafkaTemplate: ReplyingKafkaTemplate<String, String, String>) =
            ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate)
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
    // -- must NOT be picked up by replyingKafkaCoroutineTemplate(), since the qualifier scopes the lookup.
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
    fun `wires a ReplyingKafkaCoroutineTemplate when a ReplyingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(ReplyingKafkaTemplateConfig::class.java, ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertTrue(context.containsBean("replyingKafkaCoroutineTemplate"))
        context.getBean(ReplyingKafkaCoroutineTemplate::class.java)
    }

    @Test
    fun `does not wire a ReplyingKafkaCoroutineTemplate when no ReplyingKafkaTemplate is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java)
        context.refresh()

        assertFalse(context.containsBean("replyingKafkaCoroutineTemplate"))
    }

    @Test
    fun `backs off when a ReplyingKafkaCoroutineTemplate bean is already present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ReplyingKafkaTemplateConfig::class.java,
            CustomReplyingKafkaCoroutineTemplateConfig::class.java,
            ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val custom = context.getBean("customReplyingKafkaCoroutineTemplate", ReplyingKafkaCoroutineTemplate::class.java)
        val wired = context.getBean(ReplyingKafkaCoroutineTemplate::class.java)

        assertSame(custom, wired)
        assertFalse(context.containsBean("replyingKafkaCoroutineTemplate"))
    }

    @Test
    @Timeout(10)
    suspend fun `uses Dispatchers-IO by default when no qualified dispatcher bean is present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingReplyingKafkaTemplateConfig::class.java,
            ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingReplyingKafkaTemplate::class.java)
        val callingThreadName = currentThreadName()
        val coroutineTemplate = context.getBean(ReplyingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.waitForAssignment(1.seconds)

        assertFalse(delegate.lastWaitForAssignmentThreadName == callingThreadName)
        assertTrue(delegate.lastWaitForAssignmentThreadName?.contains("DefaultDispatcher") == true)
    }

    @Test
    @Timeout(10)
    suspend fun `uses the qualified CoroutineDispatcher bean when present`() {
        context = AnnotationConfigApplicationContext()
        context.register(
            ThreadRecordingReplyingKafkaTemplateConfig::class.java,
            UnqualifiedDispatcherConfig::class.java,
            QualifiedDispatcherConfig::class.java,
            ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java,
        )
        context.refresh()

        val delegate = context.getBean(ThreadRecordingReplyingKafkaTemplate::class.java)
        val coroutineTemplate = context.getBean(ReplyingKafkaCoroutineTemplate::class.java)

        coroutineTemplate.waitForAssignment(1.seconds)

        assertEquals(true, delegate.lastWaitForAssignmentThreadName?.startsWith("receive-test-thread"))
    }
}
