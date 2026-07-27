package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.Deferred
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.support.SendResult
import org.springframework.messaging.Message
import kotlin.time.Duration

/**
 * The result of one of the [ReplyingKafkaCoroutineOperations.sendAndReceiveDeferred] overloads: the
 * request-publish outcome and the correlated reply, as independent [Deferred]s so a caller can await
 * either one without waiting for the other -- e.g. to confirm the request was published without caring
 * whether (or how long) the reply takes.
 * @param sendResult completes once the request has been acknowledged by the broker.
 * @param reply completes once the correlated reply arrives, or fails/is cancelled if the request times
 * out, fails to send, or is cancelled.
 */
public data class SendAndReceiveResult<K, V, R>(
    val sendResult: Deferred<SendResult<K, V>>,
    val reply: Deferred<R>,
)

// Named sendAndReceiveTyped/sendAndReceiveDeferredTyped rather than overloading sendAndReceive/
// sendAndReceiveDeferred: Kotlin always prefers a member function over an extension with the same name
// once one is otherwise callable by its value-parameter shape, regardless of a differing (or absent)
// type-parameter list -- since the existing single-Message-parameter member already matches, an
// identically-named extension taking the same single Message<*> parameter could never be selected, even
// with an explicit type argument at the call site. A distinct name sidesteps that shadowing rule
// entirely.

/**
 * Reified convenience for the typed [ReplyingKafkaCoroutineOperations.sendAndReceive] overload -- builds
 * the [ParameterizedTypeReference] from the reified type argument instead of requiring the caller to
 * write `object : ParameterizedTypeReference<P>() {}` by hand.
 * @param message the message to send.
 * @return the reply message, with payload of type [P].
 */
public suspend inline fun <reified P, K, V, R> ReplyingKafkaCoroutineOperations<K, V, R>.sendAndReceiveTyped(
    message: Message<*>,
): Message<P> = sendAndReceive(message, object : ParameterizedTypeReference<P>() {})

/**
 * Reified convenience for the typed [ReplyingKafkaCoroutineOperations.sendAndReceive] overload -- builds
 * the [ParameterizedTypeReference] from the reified type argument instead of requiring the caller to
 * write `object : ParameterizedTypeReference<P>() {}` by hand.
 * @param message the message to send.
 * @param replyTimeout the reply timeout.
 * @return the reply message, with payload of type [P].
 */
public suspend inline fun <reified P, K, V, R> ReplyingKafkaCoroutineOperations<K, V, R>.sendAndReceiveTyped(
    message: Message<*>,
    replyTimeout: Duration,
): Message<P> = sendAndReceive(message, replyTimeout, object : ParameterizedTypeReference<P>() {})

/**
 * Reified convenience for the typed [ReplyingKafkaCoroutineOperations.sendAndReceiveDeferred] overload --
 * builds the [ParameterizedTypeReference] from the reified type argument instead of requiring the caller
 * to write `object : ParameterizedTypeReference<P>() {}` by hand.
 * @param message the message to send.
 * @return the send result and reply message, as independent deferreds.
 */
public inline fun <reified P, K, V, R> ReplyingKafkaCoroutineOperations<K, V, R>.sendAndReceiveDeferredTyped(
    message: Message<*>,
): SendAndReceiveResult<K, V, Message<P>> = sendAndReceiveDeferred(message, object : ParameterizedTypeReference<P>() {})

/**
 * Reified convenience for the typed [ReplyingKafkaCoroutineOperations.sendAndReceiveDeferred] overload --
 * builds the [ParameterizedTypeReference] from the reified type argument instead of requiring the caller
 * to write `object : ParameterizedTypeReference<P>() {}` by hand.
 * @param message the message to send.
 * @param replyTimeout the reply timeout.
 * @return the send result and reply message, as independent deferreds.
 */
public inline fun <reified P, K, V, R> ReplyingKafkaCoroutineOperations<K, V, R>.sendAndReceiveDeferredTyped(
    message: Message<*>,
    replyTimeout: Duration,
): SendAndReceiveResult<K, V, Message<P>> = sendAndReceiveDeferred(message, replyTimeout, object : ParameterizedTypeReference<P>() {})
