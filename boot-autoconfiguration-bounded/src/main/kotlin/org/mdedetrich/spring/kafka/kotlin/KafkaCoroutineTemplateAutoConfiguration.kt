package org.mdedetrich.spring.kafka.kotlin

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate

/**
 * Exposes a [KafkaCoroutineTemplate] bean built from the application's own [KafkaTemplate] (e.g. the one
 * Spring Boot's own `KafkaAutoConfiguration` already provides), so it can be autowired directly instead of
 * every caller constructing one by hand. Gated on [ConditionalOnSingleCandidate] rather than
 * `@ConditionalOnBean` -- with more than one [KafkaTemplate] bean and none marked `@Primary`,
 * `@ConditionalOnBean` would still match (it only checks that at least one exists), but the `@Bean` method
 * below injects an unqualified [KafkaTemplate], which Spring can't resolve unambiguously; the whole context
 * then fails to refresh with `UnsatisfiedDependencyException: ... expected single matching bean but found N`
 * (confirmed with a real multi-template `ApplicationContextRunner` test). `@ConditionalOnSingleCandidate`
 * backs this auto-configuration off entirely in that case (matching Boot's own auto-configurations that face
 * the same "which bean of this type" problem, e.g. `DataSource`), rather than crashing the whole application.
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(KafkaAutoConfiguration::class)
@ConditionalOnSingleCandidate(KafkaTemplate::class)
public class KafkaCoroutineTemplateAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(KafkaCoroutineTemplate::class)
    public fun <K : Any, V : Any> kafkaCoroutineTemplate(
        kafkaTemplate: KafkaTemplate<K, V>,
        @BlockingIODispatcher blockingIODispatcher: ObjectProvider<CoroutineDispatcher>,
    ): KafkaCoroutineTemplate<K, V> = KafkaCoroutineTemplate(kafkaTemplate, blockingIODispatcher.getIfUnique { Dispatchers.IO })
}
