package org.mdedetrich.spring.kafka.kotlin

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.context.annotation.ImportCandidates

/**
 * Confirms [KafkaCoroutineTemplateAutoConfiguration]/[ReplyingKafkaCoroutineTemplateAutoConfiguration]/
 * [RoutingKafkaCoroutineTemplateAutoConfiguration]/[AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration]
 * are actually registered via
 * `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` -- the mechanism
 * [org.springframework.boot.autoconfigure.AutoConfigurationImportSelector] itself uses to discover them.
 * Being merely a `@Configuration` class with Boot's conditional annotations does not make it
 * auto-discovered without this file; a consuming application would otherwise have to `@Import` it
 * manually.
 */
class AutoConfigurationImportsTest {
    @Test
    fun `KafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader).candidates

        assertTrue(candidates.contains(KafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `ReplyingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader).candidates

        assertTrue(candidates.contains(ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `RoutingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader).candidates

        assertTrue(candidates.contains(RoutingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }

    @Test
    fun `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration is registered for auto-discovery`() {
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader).candidates

        assertTrue(candidates.contains(AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name))
    }
}
