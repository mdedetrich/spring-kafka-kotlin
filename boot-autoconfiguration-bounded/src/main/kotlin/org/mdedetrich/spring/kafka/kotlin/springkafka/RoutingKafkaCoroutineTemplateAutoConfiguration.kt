package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.RoutingKafkaTemplate

/**
 * Exposes a [RoutingKafkaCoroutineTemplate] bean built from the application's own [RoutingKafkaTemplate]
 * bean. Unlike [org.springframework.kafka.core.KafkaTemplate], Spring Boot's own `KafkaAutoConfiguration`
 * never provides a [RoutingKafkaTemplate] bean automatically -- it needs an application-specific map of
 * topic-pattern to [org.springframework.kafka.core.ProducerFactory] -- so this only activates once the
 * application has registered its own [RoutingKafkaTemplate] bean (required anyway for that bean's own
 * [org.springframework.context.ApplicationContextAware]/[org.springframework.beans.factory.BeanNameAware]/
 * [org.springframework.beans.factory.SmartInitializingSingleton] callbacks to fire; see
 * [RoutingKafkaCoroutineTemplate]'s own doc). [RoutingKafkaTemplate] is fixed to
 * `KafkaTemplate<Object, Object>` (not generic), so unlike [KafkaCoroutineTemplateAutoConfiguration]/
 * [ReplyingKafkaCoroutineTemplateAutoConfiguration] this `@Bean` method has no type parameters of its own,
 * and this file is identical across the unbounded/bounded split -- it's shared into both
 * `boot-autoconfiguration-unbounded` and `boot-autoconfiguration-bounded` unchanged. Gated on
 * [ConditionalOnSingleCandidate] rather than `@ConditionalOnBean` for the same reason as
 * [KafkaCoroutineTemplateAutoConfiguration] -- multiple [RoutingKafkaTemplate] beans with none `@Primary`
 * would otherwise pass the condition but crash context refresh on the `@Bean` method's unqualified injection.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnSingleCandidate(RoutingKafkaTemplate::class)
public class RoutingKafkaCoroutineTemplateAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(RoutingKafkaCoroutineTemplate::class)
    public fun routingKafkaCoroutineTemplate(
        routingKafkaTemplate: RoutingKafkaTemplate,
        @BlockingIODispatcher blockingIODispatcher: ObjectProvider<CoroutineDispatcher>,
    ): RoutingKafkaCoroutineTemplate =
        RoutingKafkaCoroutineTemplate(routingKafkaTemplate, blockingIODispatcher.getIfUnique { Dispatchers.IO })
}
