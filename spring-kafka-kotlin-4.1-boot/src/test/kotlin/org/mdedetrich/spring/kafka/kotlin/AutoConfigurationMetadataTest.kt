package org.mdedetrich.spring.kafka.kotlin

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Properties

/**
 * Confirms `spring-boot-autoconfigure-processor` (applied via kapt, see this module's own
 * build.gradle.kts) actually generates `META-INF/spring-autoconfigure-metadata.properties` on this
 * module's own classpath, and that its contents correctly capture each auto-configuration's real
 * condition -- not just that the file exists (already confirmed by inspecting the built jar directly),
 * but that its properties are correct. Stays per-module rather than shared, same reasoning as
 * [AutoConfigurationImportsTest]: the file itself is inherently per-module kapt output, not something a
 * shared `boot-dispatcher-test-*`/`boot-autoconfiguration-*` module could produce on its own standalone
 * classpath -- neither applies kapt/`spring-boot-autoconfigure-processor` (see those modules' own
 * build.gradle.kts for why).
 */
class AutoConfigurationMetadataTest {
    private val properties: Properties by lazy {
        val stream =
            requireNotNull(javaClass.classLoader.getResourceAsStream("META-INF/spring-autoconfigure-metadata.properties")) {
                "META-INF/spring-autoconfigure-metadata.properties was not found on the classpath"
            }
        stream.use { Properties().apply { load(it) } }
    }

    @Test
    fun `captures KafkaCoroutineTemplateAutoConfiguration's ConditionalOnSingleCandidate and AutoConfigureAfter`() {
        val className = KafkaCoroutineTemplateAutoConfiguration::class.java.name
        assertEquals(
            "org.springframework.kafka.core.KafkaTemplate",
            properties.getProperty("$className.ConditionalOnSingleCandidate"),
        )
        assertEquals(
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
            properties.getProperty("$className.AutoConfigureAfter"),
        )
    }

    @Test
    fun `captures ReplyingKafkaCoroutineTemplateAutoConfiguration's ConditionalOnSingleCandidate`() {
        val className = ReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name
        assertEquals(
            "org.springframework.kafka.requestreply.ReplyingKafkaTemplate",
            properties.getProperty("$className.ConditionalOnSingleCandidate"),
        )
    }

    @Test
    fun `captures RoutingKafkaCoroutineTemplateAutoConfiguration's ConditionalOnSingleCandidate`() {
        val className = RoutingKafkaCoroutineTemplateAutoConfiguration::class.java.name
        assertEquals(
            "org.springframework.kafka.core.RoutingKafkaTemplate",
            properties.getProperty("$className.ConditionalOnSingleCandidate"),
        )
    }

    @Test
    fun `captures AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration's ConditionalOnSingleCandidate`() {
        val className = AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration::class.java.name
        assertEquals(
            "org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate",
            properties.getProperty("$className.ConditionalOnSingleCandidate"),
        )
    }
}
