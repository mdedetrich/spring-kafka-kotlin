package org.mdedetrich.spring.kafka.kotlin.springkafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import java.util.function.Function

/**
 * Shared `getMicrometerTagsProvider` caching implementation for [KafkaCoroutineTemplate] and
 * [ReplyingKafkaCoroutineTemplate] -- both ultimately wrap a real [KafkaTemplate] (directly, or via
 * [org.springframework.kafka.requestreply.ReplyingKafkaTemplate] extending it) and need the exact
 * same wrapper-lambda cache, so the implementation lives here once instead of being duplicated.
 *
 * Star-projected, not `<K, V>`: this class never touches a key or value, only the template's
 * [KafkaTemplate.micrometerTagsProvider] property, and staying generic-free sidesteps spring-kafka 4.0+ marking
 * `org.springframework.kafka.core` as `@NullMarked` (JSpecify) -- [KafkaTemplate]'s own type parameters
 * become implicitly non-null there, which would conflict with 2.9 through 3.3's [KafkaCoroutineTemplate]
 * etc. deliberately keeping K/V nullable-capable (e.g. `key: K & Any` alongside `SendResult<K?, V?>`).
 */
internal class MicrometerTagsProviderCache(
    private val kafkaTemplate: KafkaTemplate<*, *>,
) {
    // rawMicrometerTagsProvider is the last-seen kafkaTemplate.micrometerTagsProvider instance,
    // wrappedMicrometerTagsProvider is the Kotlin lambda wrapping it. Avoids allocating a new wrapper
    // lambda on every call; the identity check against kafkaTemplate's live value still catches
    // changes made directly on the delegate.
    private var rawMicrometerTagsProvider: Function<ProducerRecord<*, *>, Map<String, String>>? = null
    private var wrappedMicrometerTagsProvider: ((ProducerRecord<*, *>) -> Map<String, String>)? = null

    fun get(): ((ProducerRecord<*, *>) -> Map<String, String>)? {
        val raw = kafkaTemplate.micrometerTagsProvider
        if (raw !== rawMicrometerTagsProvider) {
            rawMicrometerTagsProvider = raw
            wrappedMicrometerTagsProvider = raw?.let { fn -> { record: ProducerRecord<*, *> -> fn.apply(record) } }
        }
        return wrappedMicrometerTagsProvider
    }
}
