package org.mdedetrich.spring.kafka.kotlin

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.core.io.support.SpringFactoriesLoader

/**
 * Confirms [KafkaCoroutineTemplateAutoConfiguration]/[ReplyingKafkaCoroutineTemplateAutoConfiguration]/
 * [RoutingKafkaCoroutineTemplateAutoConfiguration]/[AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration]
 * are actually registered via `META-INF/spring.factories` -- Spring Boot 2.6 predates the
 * `AutoConfiguration.imports` file mechanism (added in Boot 2.7, see the newer modules' own version of
 * this test), so auto-configurations here are still discovered the legacy way, keyed under
 * `EnableAutoConfiguration`. Being merely a `@Configuration` class with Boot's conditional annotations does
 * not make it auto-discovered without this entry; a consuming application would otherwise have to
 * `@Import` it manually.
 */
class AutoConfigurationImportsTest {
    @Test
    fun `KafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = SpringFactoriesLoader.loadFactoryNames(EnableAutoConfiguration::class.java, javaClass.classLoader)

        assertTrue(candidates.contains(KafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `ReplyingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = SpringFactoriesLoader.loadFactoryNames(EnableAutoConfiguration::class.java, javaClass.classLoader)

        assertTrue(candidates.contains(ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `RoutingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = SpringFactoriesLoader.loadFactoryNames(EnableAutoConfiguration::class.java, javaClass.classLoader)

        assertTrue(candidates.contains(RoutingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = SpringFactoriesLoader.loadFactoryNames(EnableAutoConfiguration::class.java, javaClass.classLoader)

        assertTrue(candidates.contains(AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }
}
