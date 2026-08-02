package org.mdedetrich.spring.kafka.kotlin

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.requestreply.CorrelationKey
import org.springframework.messaging.Message
import org.springframework.scheduling.TaskScheduler
import kotlin.time.Duration

/**
 * The basic request/reply Kafka operations contract, coroutine-native counterpart to
 * [org.springframework.kafka.requestreply.ReplyingKafkaOperations] -- `suspend fun` in place of
 * [org.springframework.kafka.requestreply.RequestReplyFuture], which on this version extends
 * [org.springframework.util.concurrent.SettableListenableFuture] (a plain
 * [java.util.concurrent.CompletableFuture] from spring-kafka 3.0).
 *
 * @param <K> the request key type.
 * @param <V> the request value type.
 * @param <R> the reply value type.
 */
public interface ReplyingKafkaCoroutineOperations<K, V, R> {
    /**
     * Wait for the reply container to have the required number of assigned partitions.
     * @param duration how long to wait.
     * @return true if assigned within the duration.
     */
    public suspend fun waitForAssignment(duration: Duration): Boolean

    /**
     * Send the request and receive a reply.
     * @param record the record to send.
     * @return the reply record.
     */
    public suspend fun sendAndReceive(record: ProducerRecord<K, V>): ConsumerRecord<K, R>

    /**
     * Send the request and receive a reply.
     * @param record the record to send.
     * @param replyTimeout the reply timeout.
     * @return the reply record.
     */
    public suspend fun sendAndReceive(
        record: ProducerRecord<K, V>,
        replyTimeout: Duration,
    ): ConsumerRecord<K, R>

    /**
     * Send the request and receive a reply message.
     * @param message the message to send.
     * @return the reply message.
     */
    public suspend fun sendAndReceive(message: Message<*>): Message<*>

    /**
     * Send the request and receive a reply message.
     * @param message the message to send.
     * @param replyTimeout the reply timeout.
     * @return the reply message.
     */
    public suspend fun sendAndReceive(
        message: Message<*>,
        replyTimeout: Duration,
    ): Message<*>

    /**
     * Send the request and receive a reply message, converting its payload to the type described by
     * [typeReference].
     * @param message the message to send.
     * @param typeReference the type to convert the reply payload to.
     * @return the reply message, with payload of type [P].
     */
    public suspend fun <P> sendAndReceive(
        message: Message<*>,
        typeReference: ParameterizedTypeReference<P>,
    ): Message<P>

    /**
     * Send the request and receive a reply message, converting its payload to the type described by
     * [typeReference].
     * @param message the message to send.
     * @param replyTimeout the reply timeout.
     * @param typeReference the type to convert the reply payload to.
     * @return the reply message, with payload of type [P].
     */
    public suspend fun <P> sendAndReceive(
        message: Message<*>,
        replyTimeout: Duration,
        typeReference: ParameterizedTypeReference<P>,
    ): Message<P>

    /**
     * Like [sendAndReceive], but returns immediately with the send outcome and reply as independent
     * [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round trip.
     * @param record the record to send.
     * @return the send result and reply, as independent deferreds.
     */
    public fun sendAndReceiveDeferred(record: ProducerRecord<K, V>): SendAndReceiveResult<K, V, ConsumerRecord<K, R>>

    /**
     * Like [sendAndReceive], but returns immediately with the send outcome and reply as independent
     * [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round trip.
     * @param record the record to send.
     * @param replyTimeout the reply timeout.
     * @return the send result and reply, as independent deferreds.
     */
    public fun sendAndReceiveDeferred(
        record: ProducerRecord<K, V>,
        replyTimeout: Duration,
    ): SendAndReceiveResult<K, V, ConsumerRecord<K, R>>

    /**
     * Like [sendAndReceive], but returns immediately with the send outcome and reply message as
     * independent [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round trip.
     * @param message the message to send.
     * @return the send result and reply message, as independent deferreds.
     */
    public fun sendAndReceiveDeferred(message: Message<*>): SendAndReceiveResult<K, V, Message<*>>

    /**
     * Like [sendAndReceive], but returns immediately with the send outcome and reply message as
     * independent [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round trip.
     * @param message the message to send.
     * @param replyTimeout the reply timeout.
     * @return the send result and reply message, as independent deferreds.
     */
    public fun sendAndReceiveDeferred(
        message: Message<*>,
        replyTimeout: Duration,
    ): SendAndReceiveResult<K, V, Message<*>>

    /**
     * Like the typed [sendAndReceive], but returns immediately with the send outcome and reply message
     * as independent [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round
     * trip.
     * @param message the message to send.
     * @param typeReference the type to convert the reply payload to.
     * @return the send result and reply message, as independent deferreds.
     */
    public fun <P> sendAndReceiveDeferred(
        message: Message<*>,
        typeReference: ParameterizedTypeReference<P>,
    ): SendAndReceiveResult<K, V, Message<P>>

    /**
     * Like the typed [sendAndReceive], but returns immediately with the send outcome and reply message
     * as independent [Deferred]s (see [SendAndReceiveResult]) instead of suspending for the full round
     * trip.
     * @param message the message to send.
     * @param replyTimeout the reply timeout.
     * @param typeReference the type to convert the reply payload to.
     * @return the send result and reply message, as independent deferreds.
     */
    public fun <P> sendAndReceiveDeferred(
        message: Message<*>,
        replyTimeout: Duration,
        typeReference: ParameterizedTypeReference<P>,
    ): SendAndReceiveResult<K, V, Message<P>>

    /**
     * Set the task scheduler used to schedule reply timeout timers.
     * @param scheduler the scheduler.
     */
    public fun setTaskScheduler(scheduler: TaskScheduler)

    /**
     * Set the default reply timeout used when none is provided at the call site.
     * @param defaultReplyTimeout the timeout.
     */
    public fun setDefaultReplyTimeout(defaultReplyTimeout: Duration)

    /**
     * The reply topic partitions currently assigned to the reply container.
     * @return the partitions.
     */
    public fun getAssignedReplyTopicPartitions(): Collection<TopicPartition>

    /**
     * Set to true when the reply topic is shared by multiple templates.
     * @param sharedReplyTopic true if shared.
     */
    public fun setSharedReplyTopic(sharedReplyTopic: Boolean)

    /**
     * Set a function used to determine the correlation key for an outgoing record.
     * @param correlationIdStrategy the strategy.
     */
    public fun setCorrelationIdStrategy(correlationIdStrategy: (ProducerRecord<K, V>) -> CorrelationKey)

    /**
     * Set the header name used to carry the correlation id.
     * @param correlationHeaderName the header name.
     */
    public fun setCorrelationHeaderName(correlationHeaderName: String)

    /**
     * Set the header name used to carry the reply topic.
     * @param replyTopicHeaderName the header name.
     */
    public fun setReplyTopicHeaderName(replyTopicHeaderName: String)

    /**
     * Set the header name used to carry the reply partition.
     * @param replyPartitionHeaderName the header name.
     */
    public fun setReplyPartitionHeaderName(replyPartitionHeaderName: String)

    /**
     * Set a function used to check whether a reply record represents an error, returning the
     * exception to raise for it, or null if the reply is not an error.
     * @param replyErrorChecker the checker.
     */
    public fun setReplyErrorChecker(replyErrorChecker: (ConsumerRecord<*, *>) -> Exception?)
}
