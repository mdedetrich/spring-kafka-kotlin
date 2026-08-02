package org.mdedetrich.spring.kafka.kotlin.springkafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.RoutingKafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import java.util.regex.Pattern

// A minimal ProducerFactory that's never actually invoked -- these tests only verify bean wiring and
// auto-configuration processing order via Spring Boot's own real auto-configuration discovery pipeline,
// not real message sending.
private class OrderingFakeProducerFactory : ProducerFactory<Any, Any> {
    override fun createProducer(): Producer<Any, Any> = throw UnsupportedOperationException("not used in this test")
}

private fun orderingConsumerFactory() =
    DefaultKafkaConsumerFactory<Any, Any>(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ),
    )

private fun orderingUnstartedReplyContainer(): ConcurrentMessageListenerContainer<Any, Any> {
    val container = ConcurrentMessageListenerContainer<Any, Any>(orderingConsumerFactory(), ContainerProperties("replies"))
    container.isAutoStartup = false
    return container
}

private fun orderingUnstartedAggregatingReplyContainer(): ConcurrentMessageListenerContainer<Any, Collection<ConsumerRecord<Any, Any>>> {
    val containerProperties = ContainerProperties("replies").apply { ackMode = ContainerProperties.AckMode.MANUAL }
    val container =
        ConcurrentMessageListenerContainer<Any, Collection<ConsumerRecord<Any, Any>>>(orderingConsumerFactory(), containerProperties)
    container.isAutoStartup = false
    return container
}

@Configuration(proxyBeanMethods = false)
private class OrderingReplyingKafkaTemplateConfig {
    @Bean
    fun replyingKafkaTemplate(): ReplyingKafkaTemplate<Any, Any, Any> =
        ReplyingKafkaTemplate(OrderingFakeProducerFactory(), orderingUnstartedReplyContainer()).apply { isAutoStartup = false }
}

@Configuration(proxyBeanMethods = false)
private class OrderingRoutingKafkaTemplateConfig {
    @Bean
    fun routingKafkaTemplate(): RoutingKafkaTemplate =
        RoutingKafkaTemplate(
            mapOf(
                Pattern.compile("orders.*") to OrderingFakeProducerFactory(),
            ),
        )
}

@Configuration(proxyBeanMethods = false)
private class OrderingAggregatingReplyingKafkaTemplateConfig {
    @Bean
    fun aggregatingReplyingKafkaTemplate(): AggregatingReplyingKafkaTemplate<Any, Any, Any> =
        AggregatingReplyingKafkaTemplate(
            OrderingFakeProducerFactory(),
            orderingUnstartedAggregatingReplyContainer(),
        ) { records, _ -> records.size >= 2 }.apply { isAutoStartup = false }
}

// Two KafkaTemplate beans, neither @Primary -- the ambiguous case that used to crash context refresh.
@Configuration(proxyBeanMethods = false)
private class OrderingTwoKafkaTemplatesConfig {
    @Bean
    fun kafkaTemplateA(): KafkaTemplate<Any, Any> = KafkaTemplate(OrderingFakeProducerFactory())

    @Bean
    fun kafkaTemplateB(): KafkaTemplate<Any, Any> = KafkaTemplate(OrderingFakeProducerFactory())
}

// Same as above, but one bean is @Primary -- ConditionalOnSingleCandidate should still resolve and wire.
@Configuration(proxyBeanMethods = false)
private class OrderingTwoKafkaTemplatesWithPrimaryConfig {
    @Bean
    @Primary
    fun kafkaTemplateA(): KafkaTemplate<Any, Any> = KafkaTemplate(OrderingFakeProducerFactory())

    @Bean
    fun kafkaTemplateB(): KafkaTemplate<Any, Any> = KafkaTemplate(OrderingFakeProducerFactory())
}

@Configuration(proxyBeanMethods = false)
private class OrderingTwoReplyingKafkaTemplatesConfig {
    @Bean
    fun replyingKafkaTemplateA(): ReplyingKafkaTemplate<Any, Any, Any> =
        ReplyingKafkaTemplate(OrderingFakeProducerFactory(), orderingUnstartedReplyContainer()).apply { isAutoStartup = false }

    @Bean
    fun replyingKafkaTemplateB(): ReplyingKafkaTemplate<Any, Any, Any> =
        ReplyingKafkaTemplate(OrderingFakeProducerFactory(), orderingUnstartedReplyContainer()).apply { isAutoStartup = false }
}

@Configuration(proxyBeanMethods = false)
private class OrderingTwoRoutingKafkaTemplatesConfig {
    @Bean
    fun routingKafkaTemplateA(): RoutingKafkaTemplate =
        RoutingKafkaTemplate(
            mapOf(
                Pattern.compile("orders.*") to OrderingFakeProducerFactory(),
            ),
        )

    @Bean
    fun routingKafkaTemplateB(): RoutingKafkaTemplate =
        RoutingKafkaTemplate(mapOf(Pattern.compile("payments.*") to OrderingFakeProducerFactory()))
}

@Configuration(proxyBeanMethods = false)
private class OrderingTwoAggregatingReplyingKafkaTemplatesConfig {
    @Bean
    fun aggregatingReplyingKafkaTemplateA(): AggregatingReplyingKafkaTemplate<Any, Any, Any> =
        AggregatingReplyingKafkaTemplate(
            OrderingFakeProducerFactory(),
            orderingUnstartedAggregatingReplyContainer(),
        ) { records, _ -> records.size >= 2 }.apply { isAutoStartup = false }

    @Bean
    fun aggregatingReplyingKafkaTemplateB(): AggregatingReplyingKafkaTemplate<Any, Any, Any> =
        AggregatingReplyingKafkaTemplate(
            OrderingFakeProducerFactory(),
            orderingUnstartedAggregatingReplyContainer(),
        ) { records, _ -> records.size >= 2 }.apply { isAutoStartup = false }
}

/**
 * Confirms each auto-configuration actually wires its coroutine template bean when run through Spring
 * Boot's own real auto-configuration discovery/ordering pipeline (`ApplicationContextRunner` +
 * `AutoConfigurations.of(...)`), not just this repo's own manual `context.register(...)`-based tests --
 * which bypass `AutoConfigurationImportSelector`/`AutoConfigurationSorter` entirely and cannot catch
 * ordering bugs between auto-configuration classes.
 *
 * Regression coverage for a real bug this approach caught: `KafkaCoroutineTemplateAutoConfiguration`
 * silently never fired without `@AutoConfigureAfter(KafkaAutoConfiguration::class)`, since
 * `AutoConfigurationSorter`'s alphabetical fallback ordering (`org.mdedetrich...` sorts before
 * `org.springframework...`) processed it before Boot's own `KafkaAutoConfiguration` had registered the
 * `KafkaTemplate` bean its `@ConditionalOnBean` depends on. The other three auto-configurations don't need
 * the same fix -- their delegate beans are always user-defined, never created by another
 * auto-configuration, so Boot's own guarantee ("`@ConditionalOnBean`/`@ConditionalOnMissingBean` on
 * auto-configuration classes load after any user-defined bean definitions") already covers them -- proven
 * here rather than just assumed.
 *
 * Also regression coverage for a second real bug: all four auto-configurations used `@ConditionalOnBean`
 * (only checks "at least one bean of this type exists") combined with an unqualified injection of that same
 * type in their `@Bean` method. With more than one delegate bean and none marked `@Primary`, the condition
 * still passed, but context refresh then crashed with `UnsatisfiedDependencyException: ... expected single
 * matching bean but found N` -- reproduced here before the fix. Switched to `@ConditionalOnSingleCandidate`,
 * which backs the auto-configuration off cleanly in the ambiguous case (proven below) while still resolving
 * correctly when one delegate is `@Primary` (also proven below).
 */
class AutoConfigurationOrderingTest {
    @Test
    fun `KafkaCoroutineTemplateAutoConfiguration wires when run after Boot's own KafkaAutoConfiguration`() {
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(KafkaAutoConfiguration::class.java, KafkaCoroutineTemplateAutoConfiguration::class.java),
            ).run { context ->
                assertTrue(context.getBeansOfType(KafkaTemplate::class.java).isNotEmpty())
                assertTrue(context.getBeansOfType(KafkaCoroutineTemplate::class.java).isNotEmpty())
            }
    }

    @Test
    fun `ReplyingKafkaCoroutineTemplateAutoConfiguration wires with a user-defined delegate, no Boot auto-configuration needed`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingReplyingKafkaTemplateConfig::class.java)
            .withConfiguration(AutoConfigurations.of(ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.getBeansOfType(ReplyingKafkaTemplate::class.java).isNotEmpty())
                assertTrue(context.getBeansOfType(ReplyingKafkaCoroutineTemplate::class.java).isNotEmpty())
            }
    }

    @Test
    fun `RoutingKafkaCoroutineTemplateAutoConfiguration wires with a user-defined delegate, no Boot auto-configuration needed`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingRoutingKafkaTemplateConfig::class.java)
            .withConfiguration(AutoConfigurations.of(RoutingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.getBeansOfType(RoutingKafkaTemplate::class.java).isNotEmpty())
                assertTrue(context.getBeansOfType(RoutingKafkaCoroutineTemplate::class.java).isNotEmpty())
            }
    }

    @Test
    fun `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration wires with a user-defined delegate`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingAggregatingReplyingKafkaTemplateConfig::class.java)
            .withConfiguration(AutoConfigurations.of(AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.getBeansOfType(AggregatingReplyingKafkaTemplate::class.java).isNotEmpty())
                assertTrue(context.getBeansOfType(AggregatingReplyingKafkaCoroutineTemplate::class.java).isNotEmpty())
            }
    }

    @Test
    fun `KafkaCoroutineTemplateAutoConfiguration backs off cleanly with multiple KafkaTemplate beans and no Primary`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingTwoKafkaTemplatesConfig::class.java)
            .withConfiguration(
                AutoConfigurations.of(KafkaAutoConfiguration::class.java, KafkaCoroutineTemplateAutoConfiguration::class.java),
            ).run { context ->
                assertTrue(context.startupFailure == null)
                assertTrue(context.getBeansOfType(KafkaCoroutineTemplate::class.java).isEmpty())
            }
    }

    @Test
    fun `KafkaCoroutineTemplateAutoConfiguration wires the Primary KafkaTemplate when multiple beans exist`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingTwoKafkaTemplatesWithPrimaryConfig::class.java)
            .withConfiguration(
                AutoConfigurations.of(KafkaAutoConfiguration::class.java, KafkaCoroutineTemplateAutoConfiguration::class.java),
            ).run { context ->
                assertTrue(context.startupFailure == null)
                assertTrue(context.getBeansOfType(KafkaCoroutineTemplate::class.java).isNotEmpty())
            }
    }

    @Test
    fun `ReplyingKafkaCoroutineTemplateAutoConfiguration backs off cleanly with multiple delegate beans and no Primary`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingTwoReplyingKafkaTemplatesConfig::class.java)
            .withConfiguration(AutoConfigurations.of(ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.startupFailure == null)
                assertTrue(context.getBeansOfType(ReplyingKafkaCoroutineTemplate::class.java).isEmpty())
            }
    }

    @Test
    fun `RoutingKafkaCoroutineTemplateAutoConfiguration backs off cleanly with multiple delegate beans and no Primary`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingTwoRoutingKafkaTemplatesConfig::class.java)
            .withConfiguration(AutoConfigurations.of(RoutingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.startupFailure == null)
                assertTrue(context.getBeansOfType(RoutingKafkaCoroutineTemplate::class.java).isEmpty())
            }
    }

    @Test
    fun `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration backs off cleanly with multiple delegate beans and no Primary`() {
        ApplicationContextRunner()
            .withUserConfiguration(OrderingTwoAggregatingReplyingKafkaTemplatesConfig::class.java)
            .withConfiguration(AutoConfigurations.of(AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java))
            .run { context ->
                assertTrue(context.startupFailure == null)
                assertTrue(context.getBeansOfType(AggregatingReplyingKafkaCoroutineTemplate::class.java).isEmpty())
            }
    }
}
