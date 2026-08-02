package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate

/**
 * Exposes a [ReplyingKafkaCoroutineTemplate] bean built from the application's own [ReplyingKafkaTemplate]
 * bean. Unlike [org.springframework.kafka.core.KafkaTemplate], Spring Boot's own `KafkaAutoConfiguration`
 * never provides a [ReplyingKafkaTemplate] bean automatically -- request/reply isn't default Kafka usage,
 * it needs an application-specific reply container/topic -- so this only activates once the application
 * has registered its own [ReplyingKafkaTemplate] bean (required anyway for its
 * [org.springframework.context.SmartLifecycle] callbacks to fire; see [ReplyingKafkaCoroutineTemplate]'s
 * own doc). Gated on [ConditionalOnSingleCandidate] rather than `@ConditionalOnBean` for the same reason as
 * [KafkaCoroutineTemplateAutoConfiguration] -- multiple [ReplyingKafkaTemplate] beans with none `@Primary`
 * would otherwise pass the condition but crash context refresh on the `@Bean` method's unqualified injection.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnSingleCandidate(ReplyingKafkaTemplate::class)
public class ReplyingKafkaCoroutineTemplateAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(ReplyingKafkaCoroutineTemplate::class)
    public fun <K : Any, V : Any, R : Any> replyingKafkaCoroutineTemplate(
        replyingKafkaTemplate: ReplyingKafkaTemplate<K, V, R>,
        @BlockingIODispatcher blockingIODispatcher: ObjectProvider<CoroutineDispatcher>,
    ): ReplyingKafkaCoroutineTemplate<K, V, R> =
        ReplyingKafkaCoroutineTemplate(replyingKafkaTemplate, blockingIODispatcher.getIfUnique { Dispatchers.IO })
}
