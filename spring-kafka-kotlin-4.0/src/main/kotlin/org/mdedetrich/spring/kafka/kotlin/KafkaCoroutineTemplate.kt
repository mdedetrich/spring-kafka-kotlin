package org.mdedetrich.spring.kafka.kotlin

import io.micrometer.observation.ObservationRegistry
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.ProducerListener
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.support.TopicPartitionOffset
import org.springframework.kafka.support.converter.RecordMessageConverter
import org.springframework.kafka.support.micrometer.KafkaTemplateObservationConvention
import org.springframework.messaging.Message
import org.springframework.messaging.converter.SmartMessageConverter
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * A template for executing high-level operations. When used with a
 * [DefaultKafkaProducerFactory], the template is thread-safe. The producer factory
 * and [org.apache.kafka.clients.producer.KafkaProducer] ensure this; refer to their
 * respective javadocs.
 *
 * [delegate] must be registered as its own Spring bean for [KafkaTemplate]'s own
 * [org.springframework.context.ApplicationContextAware]/[org.springframework.beans.factory.BeanNameAware]/[org.springframework.beans.factory.SmartInitializingSingleton] callbacks to fire normally --
 * this wrapper has no lifecycle of its own.
 *
 * Core changes from [KafkaTemplate]:
 * - Operations returning a [java.util.concurrent.CompletableFuture] are `suspend` functions instead.
 * - Durations use [kotlin.time.Duration] instead of [java.time.Duration].
 * - [receive] is a genuinely blocking call; it runs on [blockingIODispatcher] (default [Dispatchers.IO]),
 *   or the caller's own dispatcher if `null`.
 *
 * @param K the key type.
 * @param V the value type.
 * @param delegate the [KafkaTemplate] to delegate to.
 * @param blockingIODispatcher the dispatcher [receive] runs on, or `null` to run on the caller's dispatcher.
 */
public class KafkaCoroutineTemplate<K : Any, V : Any>(
    internal val delegate: KafkaTemplate<K, V>,
    private val blockingIODispatcher: CoroutineDispatcher? = Dispatchers.IO,
) : KafkaCoroutineOperations<K, V> {
    private val micrometerTagsProviderCache = MicrometerTagsProviderCache(delegate)

    /**
     * The default topic for send methods where a topic is not
     * provided.
     *
     * [KafkaTemplate.getDefaultTopic] is `@Nullable` (genuinely unset until [setDefaultTopic] is called
     * -- [sendDefault] asserts non-null internally and throws otherwise), but this property stays
     * non-null for API parity with the 2.9-3.3 wrappers: reads before ever setting a default topic fall
     * back to `""`, a value that was never a usable topic name there either.
     */
    public var defaultTopic: String
        get() = delegate.defaultTopic ?: ""
        set(value) {
            delegate.setDefaultTopic(value)
        }

    /**
     * Set a [ProducerListener] which will be invoked when Kafka acknowledges
     * a send operation. By default a [org.springframework.kafka.support.LoggingProducerListener] is configured
     * which logs errors only.
     * @param producerListener the listener
     */
    public fun setProducerListener(producerListener: ProducerListener<K, V>?): Unit = delegate.setProducerListener(producerListener)

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

    override val isTransactional: Boolean get() = delegate.isTransactional

    public val transactionIdPrefix: String? get() = delegate.transactionIdPrefix

    /**
     * Set a transaction id prefix to override the prefix in the producer factory.
     * @param transactionIdPrefix the prefix.
     * @since Spring Kafka 2.3
     */
    public fun setTransactionIdPrefix(transactionIdPrefix: String) {
        delegate.setTransactionIdPrefix(transactionIdPrefix)
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

    override val isAllowNonTransactional: Boolean get() = delegate.isAllowNonTransactional

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
     * will be added to any static tags provided in [setMicrometerTags]
     * micrometerTags}. Only applies to record listeners, ignored for batch listeners.
     * Does not apply if observation is enabled.
     * @param micrometerTagsProvider the micrometerTagsProvider.
     * @since Spring Kafka 2.9.8
     * @see [setMicrometerEnabled]
     * @see [setMicrometerTags]
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
     * Return the producer factory used by this template.
     * @return the factory.
     * @since Spring Kafka 2.2.5
     */
    override val producerFactory: ProducerFactory<K, V> get() = delegate.producerFactory

    /**
     * Return the producer factory used by this template based on the topic.
     * The default implementation returns the only producer factory.
     * @param topic the topic.
     * @return the factory.
     * @since Spring Kafka 2.5
     */
    public fun getProducerFactory(topic: String): ProducerFactory<K, V> = delegate.producerFactory

    /**
     * Set a consumer factory for receive operations.
     * @param consumerFactory the consumer factory.
     * @since Spring Kafka 2.8
     */
    public fun setConsumerFactory(consumerFactory: ConsumerFactory<K, V>): Unit = delegate.setConsumerFactory(consumerFactory)

    /**
     * Set a producer interceptor on this template.
     * @param producerInterceptor the producer interceptor
     * @since Spring Kafka 3.0
     */
    public fun setProducerInterceptor(producerInterceptor: ProducerInterceptor<K, V>): Unit =
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
     * Configure the [ObservationRegistry] to use for recording observations.
     * @param observationRegistry the observation registry to use.
     * @since Spring Kafka 3.3.1
     */
    public fun setObservationRegistry(observationRegistry: ObservationRegistry): Unit = delegate.setObservationRegistry(observationRegistry)

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
        delegate.setKafkaAdmin(kafkaAdmin)
    }

    override suspend fun sendDefault(data: V?): SendResult<K, V> = delegate.sendDefault(data).await()

    override suspend fun sendDefault(
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.sendDefault(key, data).await()

    override suspend fun sendDefault(
        partition: Int,
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.sendDefault(partition, key, data).await()

    override suspend fun sendDefault(
        partition: Int,
        timestamp: Long,
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.sendDefault(partition, timestamp, key, data).await()

    override suspend fun send(
        topic: String,
        data: V?,
    ): SendResult<K, V> = delegate.send(topic, data).await()

    override suspend fun send(
        topic: String,
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.send(topic, key, data).await()

    override suspend fun send(
        topic: String,
        partition: Int,
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.send(topic, partition, key, data).await()

    override suspend fun send(
        topic: String,
        partition: Int,
        timestamp: Long,
        key: K,
        data: V?,
    ): SendResult<K, V> = delegate.send(topic, partition, timestamp, key, data).await()

    override suspend fun send(record: ProducerRecord<K, V>): SendResult<K, V> = delegate.send(record).await()

    override suspend fun send(message: Message<*>): SendResult<K, V> = delegate.send(message).await()

    override fun partitionsFor(topic: String): List<PartitionInfo> = delegate.partitionsFor(topic)

    override val metrics: Map<MetricName, Metric> get() = delegate.metrics()

    override fun <T : Any> execute(callback: KafkaOperations.ProducerCallback<K, V, T>): T = delegate.execute(callback)

    override fun <T : Any> executeInTransaction(callback: KafkaOperations.OperationsCallback<K, V, T>): T? =
        delegate.executeInTransaction(callback)

    /**
     * **Note** It only makes sense to invoke this method if the
     * [ProducerFactory] serves up a singleton producer (such as the
     * [DefaultKafkaProducerFactory]).
     */
    override fun flush(): Unit = delegate.flush()

    override fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    ): Unit = delegate.sendOffsetsToTransaction(offsets, groupMetadata)

    override suspend fun receive(
        topic: String,
        partition: Int,
        offset: Long,
        pollTimeout: Duration,
    ): ConsumerRecord<K, V>? = withReceiveDispatcher { delegate.receive(topic, partition, offset, pollTimeout.toJavaDuration()) }

    override suspend fun receive(
        requested: Collection<TopicPartitionOffset>,
        pollTimeout: Duration,
    ): ConsumerRecords<K, V> = withReceiveDispatcher { delegate.receive(requested, pollTimeout.toJavaDuration()) }

    private suspend fun <T> withReceiveDispatcher(block: () -> T): T =
        if (blockingIODispatcher != null) withContext(blockingIODispatcher) { block() } else block()

    /**
     * Return true if the template is currently running in a transaction on the calling
     * thread.
     * @return true if a transaction is running.
     * @since Spring Kafka 2.2.1
     */
    override val inTransaction: Boolean get() = delegate.inTransaction()
}
