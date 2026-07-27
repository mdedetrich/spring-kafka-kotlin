package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate
import org.springframework.kafka.requestreply.CorrelationKey
import org.springframework.kafka.support.ProducerListener
import org.springframework.kafka.support.converter.RecordMessageConverter
import org.springframework.kafka.support.micrometer.KafkaTemplateObservationConvention
import org.springframework.messaging.Message
import org.springframework.messaging.converter.SmartMessageConverter
import org.springframework.scheduling.TaskScheduler
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * A replying template that aggregates multiple replies with the same correlation id.
 *
 * [delegate] must be registered as its own Spring bean for [AggregatingReplyingKafkaTemplate]'s own
 * [org.springframework.context.SmartLifecycle]/[org.springframework.beans.factory.InitializingBean]/[org.springframework.beans.factory.DisposableBean] callbacks to fire normally -- it owns an internal
 * reply-listening container that otherwise never starts. This wrapper has no lifecycle of its own.
 *
 * `onMessage`/`handleTimeout` aren't proxied -- they're a listener-container callback and a protected
 * subclassing hook on the real class, not application-facing API.
 *
 * Core changes from [AggregatingReplyingKafkaTemplate]:
 * - [send]/[sendDefault]/[sendAndReceive] are `suspend` functions instead of returning a
 *   [java.util.concurrent.CompletableFuture]/[org.springframework.kafka.requestreply.RequestReplyFuture].
 * - [sendAndReceiveDeferred] overloads have been added, returning a [SendAndReceiveResult] that exposes the
 *   send result and reply as independent [kotlinx.coroutines.Deferred]s -- useful when you need to await
 *   the send outcome and the reply independently.
 * - Durations use [kotlin.time.Duration] instead of [java.time.Duration].
 * - [receive]/[waitForAssignment] are genuinely blocking calls; they run on [blockingIODispatcher] (default
 *   [Dispatchers.IO]), or the caller's own dispatcher if `null`.
 *
 * @param K the key type.
 * @param V the outbound data type.
 * @param R the reply data type.
 * @param delegate the [AggregatingReplyingKafkaTemplate] to delegate to.
 * @param blockingIODispatcher the dispatcher [receive] and [waitForAssignment] run on; see [KafkaCoroutineTemplate]'s
 * parameter of the same name.
 */
public class AggregatingReplyingKafkaCoroutineTemplate<K, V, R>(
    internal val delegate: AggregatingReplyingKafkaTemplate<K, V, R>,
    private val blockingIODispatcher: CoroutineDispatcher? = Dispatchers.IO,
) : KafkaCoroutineOperations<K, V> by KafkaCoroutineTemplate(delegate, blockingIODispatcher),
    ReplyingKafkaCoroutineOperations<K, V, Collection<ConsumerRecord<K, R>>> {
    private val micrometerTagsProviderCache = MicrometerTagsProviderCache(delegate)

    /**
     * Set the timeout for committing offsets.
     * @param commitTimeout the timeout.
     */
    public fun setCommitTimeout(commitTimeout: Duration): Unit = delegate.setCommitTimeout(commitTimeout.toJavaDuration())

    /**
     * Set to true to return whatever records have been collected so far if the reply times out
     * before the release strategy is satisfied.
     * @param returnPartialOnTimeout true to return partial results.
     */
    public fun setReturnPartialOnTimeout(returnPartialOnTimeout: Boolean): Unit = delegate.setReturnPartialOnTimeout(returnPartialOnTimeout)

    // waitForAssignment blocks the calling thread (a real CountDownLatch.await under the hood) the same
    // way receive() does, so it gets the same blockingIODispatcher treatment.
    override suspend fun waitForAssignment(duration: Duration): Boolean =
        if (blockingIODispatcher != null) {
            withContext(blockingIODispatcher) { delegate.waitForAssignment(duration.toJavaDuration()) }
        } else {
            delegate.waitForAssignment(duration.toJavaDuration())
        }

    override suspend fun sendAndReceive(record: ProducerRecord<K, V>): ConsumerRecord<K, Collection<ConsumerRecord<K, R>>> =
        delegate.sendAndReceive(record).await()

    override suspend fun sendAndReceive(
        record: ProducerRecord<K, V>,
        replyTimeout: Duration,
    ): ConsumerRecord<K, Collection<ConsumerRecord<K, R>>> = delegate.sendAndReceive(record, replyTimeout.toJavaDuration()).await()

    override suspend fun sendAndReceive(message: Message<*>): Message<*> = delegate.sendAndReceive(message).await()

    override suspend fun sendAndReceive(
        message: Message<*>,
        replyTimeout: Duration,
    ): Message<*> = delegate.sendAndReceive(message, replyTimeout.toJavaDuration()).await()

    // See ReplyingKafkaCoroutineTemplate's identical overrides for why .await() and get() are both
    // needed here: .await()'s own return type erases to Message<*>, but the subsequent get() resolves
    // to the covariant Message<P> override (not the synthetic erasure bridge), non-blockingly, since the
    // future is already complete by then.
    override suspend fun <P> sendAndReceive(
        message: Message<*>,
        typeReference: ParameterizedTypeReference<P>,
    ): Message<P> {
        val future = delegate.sendAndReceive(message, typeReference)
        future.await()
        return future.get()
    }

    override suspend fun <P> sendAndReceive(
        message: Message<*>,
        replyTimeout: Duration,
        typeReference: ParameterizedTypeReference<P>,
    ): Message<P> {
        val future = delegate.sendAndReceive(message, replyTimeout.toJavaDuration(), typeReference)
        future.await()
        return future.get()
    }

    override fun sendAndReceiveDeferred(
        record: ProducerRecord<K, V>,
    ): SendAndReceiveResult<K, V, ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> {
        val future = delegate.sendAndReceive(record)
        return SendAndReceiveResult(future.sendFuture.asDeferred(), future.asDeferred())
    }

    override fun sendAndReceiveDeferred(
        record: ProducerRecord<K, V>,
        replyTimeout: Duration,
    ): SendAndReceiveResult<K, V, ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> {
        val future = delegate.sendAndReceive(record, replyTimeout.toJavaDuration())
        return SendAndReceiveResult(future.sendFuture.asDeferred(), future.asDeferred())
    }

    override fun sendAndReceiveDeferred(message: Message<*>): SendAndReceiveResult<K, V, Message<*>> {
        val future = delegate.sendAndReceive(message)
        return SendAndReceiveResult(future.sendFuture.asDeferred(), future.asDeferred())
    }

    override fun sendAndReceiveDeferred(
        message: Message<*>,
        replyTimeout: Duration,
    ): SendAndReceiveResult<K, V, Message<*>> {
        val future = delegate.sendAndReceive(message, replyTimeout.toJavaDuration())
        return SendAndReceiveResult(future.sendFuture.asDeferred(), future.asDeferred())
    }

    // See ReplyingKafkaCoroutineTemplate's identical overrides for why the typed result is delivered via
    // a manually-completed CompletableDeferred (using the covariant get() after the future is known to be
    // complete) rather than asDeferred() alone, which would infer Deferred<Message<*>>.
    override fun <P> sendAndReceiveDeferred(
        message: Message<*>,
        typeReference: ParameterizedTypeReference<P>,
    ): SendAndReceiveResult<K, V, Message<P>> {
        val future = delegate.sendAndReceive(message, typeReference)
        val reply = CompletableDeferred<Message<P>>()
        future.whenComplete { _, throwable ->
            if (throwable != null) reply.completeExceptionally(throwable) else reply.complete(future.get())
        }
        return SendAndReceiveResult(future.sendFuture.asDeferred(), reply)
    }

    override fun <P> sendAndReceiveDeferred(
        message: Message<*>,
        replyTimeout: Duration,
        typeReference: ParameterizedTypeReference<P>,
    ): SendAndReceiveResult<K, V, Message<P>> {
        val future = delegate.sendAndReceive(message, replyTimeout.toJavaDuration(), typeReference)
        val reply = CompletableDeferred<Message<P>>()
        future.whenComplete { _, throwable ->
            if (throwable != null) reply.completeExceptionally(throwable) else reply.complete(future.get())
        }
        return SendAndReceiveResult(future.sendFuture.asDeferred(), reply)
    }

    override fun setTaskScheduler(scheduler: TaskScheduler): Unit = delegate.setTaskScheduler(scheduler)

    override fun setDefaultReplyTimeout(defaultReplyTimeout: Duration): Unit =
        delegate.setDefaultReplyTimeout(defaultReplyTimeout.toJavaDuration())

    override fun getAssignedReplyTopicPartitions(): Collection<TopicPartition> = delegate.assignedReplyTopicPartitions

    override fun setSharedReplyTopic(sharedReplyTopic: Boolean): Unit = delegate.setSharedReplyTopic(sharedReplyTopic)

    override fun setCorrelationIdStrategy(correlationIdStrategy: (ProducerRecord<K, V>) -> CorrelationKey) {
        delegate.setCorrelationIdStrategy(correlationIdStrategy)
    }

    override fun setCorrelationHeaderName(correlationHeaderName: String): Unit = delegate.setCorrelationHeaderName(correlationHeaderName)

    override fun setReplyTopicHeaderName(replyTopicHeaderName: String): Unit = delegate.setReplyTopicHeaderName(replyTopicHeaderName)

    override fun setReplyPartitionHeaderName(replyPartitionHeaderName: String): Unit =
        delegate.setReplyPartitionHeaderName(replyPartitionHeaderName)

    override fun setReplyErrorChecker(replyErrorChecker: (ConsumerRecord<*, *>) -> Exception?) {
        delegate.setReplyErrorChecker(replyErrorChecker)
    }

    override fun setBinaryCorrelation(binaryCorrelation: Boolean): Unit = delegate.setBinaryCorrelation(binaryCorrelation)

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
