package org.mdedetrich.spring.kafka.kotlin

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.support.TopicPartitionOffset
import org.springframework.messaging.Message
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

/**
 * The basic Kafka operations contract returning [CompletableFuture]s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * If the Kafka topic is set with [CreateTime][org.apache.kafka.common.record.TimestampType.CREATE_TIME]
 * all send operations will use the user provided time if provided, else
 * [org.apache.kafka.clients.producer.KafkaProducer] will generate one
 *
 * If the topic is set with [LogAppendTime][org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME]
 * then the user provided timestamp will be ignored and instead will be the
 * Kafka broker local time when the message is appended
 *
 * K, V bound to Any: spring-kafka 4.0+ marks org.springframework.kafka.core/.requestreply as @NullMarked
 * (JSpecify), so KafkaTemplate<K, V>'s own type parameters are implicitly non-null there -- unlike 2.9
 * through 3.3, where K/V stayed nullable-capable (`key: K & Any` alongside `SendResult<K?, V?>`), since
 * the real delegate itself never allowed a null key or a null [SendResult] regardless. `data`/message
 * payloads stay nullable (`V?`) where the real API keeps allowing null values (tombstone records).
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Biju Kunjummen
 * @author Giacomo Baso
 */
public interface KafkaCoroutineOperations<K : Any, V : Any> {
    /**
     * Send the data to the default topic with no key or partition.
     * @param data The data.
     * @return a Future for the [SendResult].
     */
    public suspend fun sendDefault(data: V?): SendResult<K, V>

    /**
     * Send the data to the default topic with the provided key and no partition.
     * @param key the key.
     * @param data The data.
     * @return a Future for the [SendResult].
     */
    public suspend fun sendDefault(
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the default topic with the provided key and partition.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the [SendResult].
     */
    public suspend fun sendDefault(
        partition: Int,
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the default topic with the provided key and partition.
     * @param partition the partition.
     * @param timestamp the timestamp of the record.
     * @param key the key.
     * @param data the data.
     * @return a Future for the [SendResult].
     * @since Spring Kafka 1.3
     */
    public suspend fun sendDefault(
        partition: Int,
        timestamp: Long,
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the provided topic with no key or partition.
     * @param topic the topic.
     * @param data The data.
     * @return a Future for the [SendResult].
     */
    public suspend fun send(
        topic: String,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the provided topic with the provided key and no partition.
     * @param topic the topic.
     * @param key the key.
     * @param data The data.
     * @return a Future for the [SendResult].
     */
    public suspend fun send(
        topic: String,
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the [SendResult].
     */
    public suspend fun send(
        topic: String,
        partition: Int,
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param partition the partition.
     * @param timestamp the timestamp of the record.
     * @param key the key.
     * @param data the data.
     * @return a Future for the [SendResult].
     * @since Spring Kafka 1.3
     */
    public suspend fun send(
        topic: String,
        partition: Int,
        timestamp: Long,
        key: K,
        data: V?,
    ): SendResult<K, V>

    /**
     * Send the provided [ProducerRecord].
     * @param record the record.
     * @return a Future for the [SendResult].
     * @since Spring Kafka 1.3
     */
    public suspend fun send(record: ProducerRecord<K, V>): SendResult<K, V>

    /**
     * Send a message with routing information in message headers. The message payload
     * may be converted before sending.
     * @param message the message to send.
     * @return a Future for the [SendResult].
     * @see [org.springframework.kafka.support.KafkaHeaders.TOPIC]
     *
     * @see [org.springframework.kafka.support.KafkaHeaders.PARTITION]
     *
     * @see [org.springframework.kafka.support.KafkaHeaders.KEY]
     */
    public suspend fun send(message: Message<*>): SendResult<K, V>

    /**
     * See [Producer.partitionsFor].
     * @param topic the topic.
     * @return the partition info.
     * @since Spring Kafka 1.1
     */
    public fun partitionsFor(topic: String): List<PartitionInfo>

    /**
     * See [Producer.metrics].
     * @since Spring Kafka 1.1
     */
    public val metrics: Map<MetricName, Metric>

    /**
     * Execute some arbitrary operation(s) on the producer and return the result.
     * @param callback the callback.
     * @param <T> the result type.
     * @return the result.
     * @since Spring Kafka 1.1
     </T> */
    public fun <T : Any> execute(callback: KafkaOperations.ProducerCallback<K, V, T>): T

    /**
     * Execute some arbitrary operation(s) on the operations and return the result.
     * The operations are invoked within a local transaction and do not participate
     * in a global transaction (if present).
     * @param callback the callback.
     * @param <T> the result type.
     * @return the result.
     * @since Spring Kafka 1.1
     </T> */
    public fun <T : Any> executeInTransaction(callback: KafkaOperations.OperationsCallback<K, V, T>): T?

    /**
     * Flush the producer.
     */
    public fun flush()

    /**
     * When running in a transaction, send the consumer offset(s) to the transaction. It
     * is not necessary to call this method if the operations are invoked on a listener
     * container thread (and the listener container is configured with a
     * [org.springframework.kafka.transaction.KafkaAwareTransactionManager]) since
     * the container will take care of sending the offsets to the transaction.
     * Use with 2.5 brokers or later.
     * @param offsets The offsets.
     * @param groupMetadata the consumer group metadata.
     * @since Spring Kafka 2.5
     * @see [Producer.sendOffsetsToTransaction]
     */
    public fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        groupMetadata: ConsumerGroupMetadata,
    ): Unit = throw UnsupportedOperationException()

    /**
     * True if the implementation supports transactions (has a transaction-capable
     * producer factory).
     * @since Spring Kafka 2.3
     */
    public val isTransactional: Boolean

    /**
     * True if this template, when transactional, allows non-transactional operations.
     * @since Spring Kafka 2.4.3
     */
    public val isAllowNonTransactional: Boolean get() = false

    /**
     * True if the template is currently running in a transaction on the calling
     * thread.
     * @since Spring Kafka 2.5
     */
    public val inTransaction: Boolean get() = false

    /**
     * The producer factory used by this template.
     * @since Spring Kafka 2.5
     */
    public val producerFactory: ProducerFactory<K, V>
        get() = throw UnsupportedOperationException("This implementation does not support this operation")

    /**
     * Receive a single record with the default poll timeout (5 seconds).
     * @param topic the topic.
     * @param partition the partition.
     * @param offset the offset.
     * @return the record or null.
     * @since Spring Kafka 2.8
     * @see [KafkaOperations.DEFAULT_POLL_TIMEOUT]
     */
    public suspend fun receive(
        topic: String,
        partition: Int,
        offset: Long,
    ): ConsumerRecord<K, V>? = receive(topic, partition, offset, KafkaOperations.DEFAULT_POLL_TIMEOUT.toKotlinDuration())

    /**
     * Receive a single record.
     *
     * [org.springframework.kafka.core.KafkaTemplate.receive] is a genuinely blocking call (a real
     * [org.apache.kafka.clients.consumer.Consumer.poll] under the hood), unlike [send]/`sendAndReceive`
     * which just `.await()` an already-async [java.util.concurrent.CompletableFuture]. See
     * [KafkaCoroutineTemplate]'s `blockingIODispatcher` constructor parameter for how the blocking call
     * is (or isn't) moved off the calling coroutine's dispatcher.
     *
     * Genuinely nullable on this version -- `KafkaTemplate.receive(topic, partition, offset, pollTimeout)`
     * is `@Nullable` (unlike the multi-record overload below, which isn't), matching its real behavior of
     * returning nothing if the poll times out before a matching record arrives.
     * @param topic the topic.
     * @param partition the partition.
     * @param offset the offset.
     * @param pollTimeout the timeout.
     * @return the record or null.
     * @since Spring Kafka 2.8
     */
    public suspend fun receive(
        topic: String,
        partition: Int,
        offset: Long,
        pollTimeout: Duration,
    ): ConsumerRecord<K, V>?

    /**
     * Receive a multiple records with the default poll timeout (5 seconds). Only
     * absolute, positive offsets are supported.
     * @param requested a collection of record requests (topic/partition/offset).
     * @return the records
     * @since Spring Kafka 2.8
     * @see [KafkaOperations.DEFAULT_POLL_TIMEOUT]
     */
    public suspend fun receive(requested: Collection<TopicPartitionOffset>): ConsumerRecords<K, V> =
        receive(requested, KafkaOperations.DEFAULT_POLL_TIMEOUT.toKotlinDuration())

    /**
     * Receive multiple records. Only absolute, positive offsets are supported.
     * @param requested a collection of record requests (topic/partition/offset).
     * @param pollTimeout the timeout.
     * @return the record or null.
     * @since Spring Kafka 2.8
     */
    public suspend fun receive(
        requested: Collection<TopicPartitionOffset>,
        pollTimeout: Duration,
    ): ConsumerRecords<K, V>
}
