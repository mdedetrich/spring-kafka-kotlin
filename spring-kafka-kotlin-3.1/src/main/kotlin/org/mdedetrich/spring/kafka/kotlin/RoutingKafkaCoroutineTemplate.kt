package org.mdedetrich.spring.kafka.kotlin

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.RoutingKafkaTemplate
import org.springframework.kafka.support.ProducerListener
import org.springframework.kafka.support.converter.RecordMessageConverter
import org.springframework.kafka.support.micrometer.KafkaTemplateObservationConvention
import org.springframework.messaging.converter.SmartMessageConverter
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * A [org.springframework.kafka.core.KafkaTemplate] that routes messages based on the topic name. Does not
 * support transactions, [flush], [metrics], and [execute], only simple send operations.
 *
 * [delegate] must be registered as its own Spring bean for [org.springframework.kafka.core.KafkaTemplate]'s
 * own [org.springframework.context.ApplicationContextAware]/[org.springframework.beans.factory.BeanNameAware]/[org.springframework.beans.factory.SmartInitializingSingleton] callbacks to fire normally --
 * this wrapper has no lifecycle of its own.
 *
 * [RoutingKafkaTemplate] is fixed to `KafkaTemplate<Object, Object>` (not generic), so this wrapper is a
 * concrete, non-generic class too, backed by [KafkaCoroutineOperations]`<Any, Any>`.
 *
 * Core changes from [RoutingKafkaTemplate]:
 * - Operations returning a [java.util.concurrent.CompletableFuture] are `suspend` functions instead.
 * - Durations use [kotlin.time.Duration] instead of [java.time.Duration].
 * - [receive] is a genuinely blocking call; it runs on [blockingIODispatcher] (default [Dispatchers.IO]),
 *   or the caller's own dispatcher if `null`.
 *
 * @param delegate the [RoutingKafkaTemplate] to delegate to.
 * @param blockingIODispatcher the dispatcher [receive] runs on; see [KafkaCoroutineTemplate]'s parameter of
 * the same name. [RoutingKafkaTemplate] doesn't implement `waitForAssignment`, so this dispatcher only ever
 * applies to [receive] here.
 */
public class RoutingKafkaCoroutineTemplate(
    internal val delegate: RoutingKafkaTemplate,
    blockingIODispatcher: CoroutineDispatcher? = Dispatchers.IO,
) : KafkaCoroutineOperations<Any, Any> by KafkaCoroutineTemplate(delegate, blockingIODispatcher) {
    private val micrometerTagsProviderCache = MicrometerTagsProviderCache(delegate)

    /**
     * Return the producer factory that will be used to send to the given topic.
     * @param topic the topic.
     * @return the factory.
     */
    public fun getProducerFactory(topic: String): ProducerFactory<Any, Any> = delegate.getProducerFactory(topic)

    /**
     * The default topic for send methods where a topic is not
     * provided.
     */
    public var defaultTopic: String?
        get() = delegate.defaultTopic
        set(value) {
            // KafkaTemplate.setDefaultTopic is annotated non-null (@NonNullApi, no @Nullable override), but
            // its real implementation is an unconditional field write with no validation -- passing null
            // safely resets the default, matching the getter's own nullable return (the field starts
            // uninitialized/null until explicitly set).
            @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
            delegate.defaultTopic = value
        }

    /**
     * Set a [ProducerListener] which will be invoked when Kafka acknowledges
     * a send operation. By default a [org.springframework.kafka.support.LoggingProducerListener] is configured
     * which logs errors only.
     * @param producerListener the listener
     */
    public fun setProducerListener(producerListener: ProducerListener<Any, Any>?): Unit = delegate.setProducerListener(producerListener)

    /**
     * The message converter to use.
     */
    public var messageConverter: RecordMessageConverter
        get() = delegate.messageConverter
        set(value) = delegate.setMessageConverter(value)

    /**
     * Set the [SmartMessageConverter] to use with the default
     * [org.springframework.kafka.support.converter.MessagingMessageConverter]. Not allowed when a custom
     * [setMessagingConverter] is provided.
     * @param messageConverter the converter.
     * @since Spring Kafka 2.7.1
     */
    public fun setMessagingConverter(messageConverter: SmartMessageConverter): Unit = delegate.setMessagingConverter(messageConverter)

    public val transactionIdPrefix: String? get() = delegate.transactionIdPrefix

    /**
     * Set a transaction id prefix to override the prefix in the producer factory.
     * @param transactionIdPrefix the prefix.
     * @since Spring Kafka 2.3
     */
    public fun setTransactionIdPrefix(transactionIdPrefix: String) {
        delegate.transactionIdPrefix = transactionIdPrefix
    }

    /**
     * Set the maximum time to wait when closing a producer; default 5 seconds.
     * @param closeTimeout the close timeout.
     * @since Spring Kafka 2.1.14
     */
    public fun setCloseTimeout(closeTimeout: Duration): Unit = delegate.setCloseTimeout(closeTimeout.toJavaDuration())

    /**
     * Set to true to allow a non-transactional send when the template is transactional.
     * @param allowNonTransactional true to allow.
     * @since Spring Kafka 2.4.3
     */
    public fun setAllowNonTransactional(allowNonTransactional: Boolean) {
        delegate.isAllowNonTransactional = allowNonTransactional
    }

    /**
     * Set to `false` to disable micrometer timers, if micrometer is on the class path.
     * @param micrometerEnabled false to disable.
     * @since Spring Kafka 2.5
     */
    public fun setMicrometerEnabled(micrometerEnabled: Boolean): Unit = delegate.setMicrometerEnabled(micrometerEnabled)

    /**
     * Set additional tags for the Micrometer listener timers.
     * @param tags the tags.
     * @since Spring Kafka 2.5
     */
    public fun setMicrometerTags(tags: Map<String, String>): Unit = delegate.setMicrometerTags(tags)

    /**
     * Set a function to provide dynamic tags based on the producer record. These tags
     * will be added to any static tags provided in [setMicrometerTags]. Only applies to record
     * listeners, ignored for batch listeners. Does not apply if observation is enabled.
     * @param micrometerTagsProvider the micrometerTagsProvider.
     * @since Spring Kafka 2.9.8
     * @see [setMicrometerEnabled]
     * @see [setMicrometerTags]
     * @see [setObservationEnabled]
     */
    public fun setMicrometerTagsProvider(micrometerTagsProvider: ((ProducerRecord<*, *>) -> Map<String, String>)?) {
        delegate.setMicrometerTagsProvider(micrometerTagsProvider)
    }

    /**
     * Return the Micrometer tags provider.
     * @return the micrometerTagsProvider.
     * @since Spring Kafka 2.9.8
     */
    public fun getMicrometerTagsProvider(): ((ProducerRecord<*, *>) -> Map<String, String>)? = micrometerTagsProviderCache.get()

    /**
     * Set a consumer factory for receive operations.
     * @param consumerFactory the consumer factory.
     * @since Spring Kafka 2.8
     */
    public fun setConsumerFactory(consumerFactory: ConsumerFactory<Any, Any>): Unit = delegate.setConsumerFactory(consumerFactory)

    /**
     * Set a producer interceptor on this template.
     * @param producerInterceptor the producer interceptor
     * @since Spring Kafka 3.0
     */
    public fun setProducerInterceptor(producerInterceptor: ProducerInterceptor<Any, Any>): Unit =
        delegate.setProducerInterceptor(producerInterceptor)

    /**
     * Set to true to enable observation via Micrometer.
     * @param observationEnabled true to enable.
     * @since Spring Kafka 3.0
     * @see [setMicrometerEnabled]
     */
    public fun setObservationEnabled(observationEnabled: Boolean): Unit = delegate.setObservationEnabled(observationEnabled)

    /**
     * Set a custom [KafkaTemplateObservationConvention].
     * @param observationConvention the convention.
     * @since Spring Kafka 3.0
     */
    public fun setObservationConvention(observationConvention: KafkaTemplateObservationConvention): Unit =
        delegate.setObservationConvention(observationConvention)

    /**
     * Return the [KafkaAdmin], used to find the cluster id for observation, if
     * present.
     * @return the kafkaAdmin
     * @since Spring Kafka 3.0.5
     */
    public val kafkaAdmin: KafkaAdmin? get() = delegate.kafkaAdmin

    /**
     * Set the [KafkaAdmin], used to find the cluster id for observation, if
     * present.
     * @param kafkaAdmin the admin.
     */
    public fun setKafkaAdmin(kafkaAdmin: KafkaAdmin) {
        delegate.kafkaAdmin = kafkaAdmin
    }
}
