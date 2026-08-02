package org.mdedetrich.spring.kafka.kotlin

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate

/**
 * Exposes an [AggregatingReplyingKafkaCoroutineTemplate] bean built from the application's own
 * [AggregatingReplyingKafkaTemplate] bean. Same reasoning as
 * [ReplyingKafkaCoroutineTemplateAutoConfiguration]: Spring Boot's own `KafkaAutoConfiguration` never
 * provides an [AggregatingReplyingKafkaTemplate] bean automatically -- it needs an application-specific
 * reply container/topic/release strategy -- so this only activates once the application has registered its
 * own [AggregatingReplyingKafkaTemplate] bean (required anyway for its
 * [org.springframework.context.SmartLifecycle] callbacks to fire; see
 * [AggregatingReplyingKafkaCoroutineTemplate]'s own doc). Gated on [ConditionalOnSingleCandidate] rather
 * than `@ConditionalOnBean` for the same reason as [KafkaCoroutineTemplateAutoConfiguration] -- multiple
 * [AggregatingReplyingKafkaTemplate] beans with none `@Primary` would otherwise pass the condition but
 * crash context refresh on the `@Bean` method's unqualified injection.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnSingleCandidate(AggregatingReplyingKafkaTemplate::class)
public class AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(AggregatingReplyingKafkaCoroutineTemplate::class)
    public fun <K, V, R> aggregatingReplyingKafkaCoroutineTemplate(
        aggregatingReplyingKafkaTemplate: AggregatingReplyingKafkaTemplate<K, V, R>,
        @BlockingIODispatcher blockingIODispatcher: ObjectProvider<CoroutineDispatcher>,
    ): AggregatingReplyingKafkaCoroutineTemplate<K, V, R> =
        AggregatingReplyingKafkaCoroutineTemplate(aggregatingReplyingKafkaTemplate, blockingIODispatcher.getIfUnique { Dispatchers.IO })
}
