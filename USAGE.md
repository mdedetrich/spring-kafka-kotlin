# Usage: spring-kafka vs spring-kafka-kotlin

Side-by-side comparisons for the non-trivial API changes. Trivial ones (e.g. plain
`send`/`sendDefault` just becoming `suspend fun`) aren't repeated here — see
[README.md](README.md) for the summary and [IMPLEMENTATION.md](IMPLEMENTATION.md) for why.

## `send`

Plain `spring-kafka`, chaining on the returned `CompletableFuture`:

```java
kafkaTemplate.send("orders", "key-1", "payload")
    .thenAccept(result -> log.info("sent: {}", result.getRecordMetadata()))
    .exceptionally(ex -> { log.error("send failed", ex); return null; });
```

`spring-kafka-kotlin`, a direct suspend call with ordinary `try`/`catch`:

```kotlin
try {
    val result = kafkaCoroutineTemplate.send("orders", "key-1", "payload")
    log.info("sent: {}", result.recordMetadata)
} catch (ex: Exception) {
    log.error("send failed", ex)
}
```

No `.thenAccept`/`.exceptionally` chaining, and no unwrapping a `CompletionException` to
get the real cause — `.await()` (used internally) does that already.

## `receive`

Plain `spring-kafka`: a genuinely blocking call, so it must run on a thread that can
afford to block:

```java
ConsumerRecord<String, String> record = kafkaTemplate.receive("orders", 0, 0L);
```

`spring-kafka-kotlin`: a `suspend fun`, moved off the calling coroutine's thread onto a
configurable dispatcher (`Dispatchers.IO` by default):

```kotlin
val record = kafkaCoroutineTemplate.receive("orders", 0, 0L)
```

Calling it from a coroutine no longer risks blocking whatever dispatcher/thread pool the
caller happens to be running on — see [README.md](README.md#coroutines-instead-of-completablefuture)
for the `blockingIODispatcher` configuration.

## `sendAndReceive` with `ParameterizedTypeReference<P>`

Plain `spring-kafka`, blocking on the future (or chaining) and manually building the type
reference:

```java
Message<Order> reply = replyingKafkaTemplate
    .sendAndReceive(message, new ParameterizedTypeReference<Order>() {})
    .get(); // checked InterruptedException/ExecutionException
```

`spring-kafka-kotlin`, plain typed overload (manual `ParameterizedTypeReference`, same as
Java):

```kotlin
val reply: Message<Order> = replyingCoroutineTemplate
    .sendAndReceive(message, object : ParameterizedTypeReference<Order>() {})
```

`spring-kafka-kotlin`, reified convenience — no `ParameterizedTypeReference` at the call
site at all:

```kotlin
val reply: Message<Order> = replyingCoroutineTemplate.sendAndReceiveTyped(message)
```

The reified overload can't be named `sendAndReceive` (Kotlin would always resolve to the
existing member instead of the extension — see [IMPLEMENTATION.md](IMPLEMENTATION.md)), and
`P` must be inferred from an expected type at the call site (a `val` type annotation, a
function's declared return type, etc.) rather than an explicit `<Order>` type argument,
since Kotlin doesn't support specifying only some of a call's type arguments.

## Observing the send outcome independently of the reply

Plain `spring-kafka`: `RequestReplyFuture`/`RequestReplyMessageFuture` expose
`getSendFuture()` directly, since the return value already *is* a `CompletableFuture`:

```java
RequestReplyFuture<String, String, String> future = replyingKafkaTemplate.sendAndReceive(record);
future.getSendFuture().thenAccept(sendResult -> log.info("published: {}", sendResult));
ConsumerRecord<String, String> reply = future.get(); // separately, blocks for the reply
```

`spring-kafka-kotlin`: `sendAndReceive` alone can't offer this, since it suspends until the
*whole* round trip is done, collapsing send and reply into one result. Use
`sendAndReceiveDeferred` instead, which returns immediately with both halves as independent
`Deferred`s:

```kotlin
val result = replyingCoroutineTemplate.sendAndReceiveDeferred(record)
result.sendResult.await() // just the publish outcome
val reply = result.reply.await() // the correlated reply, awaited separately
```

Cancelling `result.reply` (e.g. by cancelling its enclosing coroutine) also cancels the
real underlying `CompletableFuture`, same as cancelling a coroutine awaiting a plain
`sendAndReceive` call would.

## Timeouts and durations

Plain `spring-kafka` uses `java.time.Duration`:

```java
replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(30));
```

`spring-kafka-kotlin` uses `kotlin.time.Duration`, converting at the delegate boundary:

```kotlin
replyingCoroutineTemplate.setDefaultReplyTimeout(30.seconds)
```

## `KafkaListenerCoroutineHook` vs `RecordInterceptor`

Both wrap a `@KafkaListener` invocation, but at different points, over different things —
picking between them is about *what* needs changing, not a spring-kafka-vs-spring-kafka-kotlin
API difference (`RecordInterceptor` is plain `spring-kafka`, untouched by this project).

Need to inspect, transform, or veto the `ConsumerRecord` itself before the listener ever sees
it (redact a field, validate and skip a malformed record, enrich it from an external lookup)?
That's `org.springframework.kafka.listener.adapter.RecordInterceptor` — it runs before
`processMessage`, over the record's *content*:

```kotlin
class RedactingInterceptor : RecordInterceptor<String, String> {
    override fun intercept(record: ConsumerRecord<String, String>, consumer: Consumer<String, String>) =
        if (record.value().contains("secret")) null else record // null skips the record entirely
}
```

Need to change the *environment* `processMessage` runs in instead — propagate a correlation id
via `CoroutineContext`, install a `ThreadContextElement` (e.g. MDC/`ThreadLocal` propagation
across the suspend boundary), switch dispatcher, retry the whole call, or run code both before
and after it completes? That's `KafkaListenerCoroutineHook` (see
[README.md](README.md#a-cowebfilter-style-hook-for-kafkalistener) and
[IMPLEMENTATION.md](IMPLEMENTATION.md)) — it never sees the raw `ConsumerRecord` bytes/headers
as something to rewrite, only the already-resolved `KafkaListenerInvocation` and a
`processMessage` closure to call (typically `withContext(...) { processMessage() }`):

```kotlin
@Component
class CorrelationIdHook : KafkaListenerCoroutineHook() {
    override suspend fun hook(invocation: KafkaListenerInvocation, processMessage: suspend () -> Any?): Any? {
        val correlationId = ... // extracted from invocation
        return withContext(CorrelationId(correlationId)) { processMessage() }
    }
}
```

Rule of thumb: changing what the listener *receives* is `RecordInterceptor`; changing what the
listener *runs inside of* is `KafkaListenerCoroutineHook`. The two compose fine together — a
`RecordInterceptor` transforming records and a `KafkaListenerCoroutineHook` propagating a
correlation id address unrelated concerns and can both be registered at once.

## Nullability

Plain `spring-kafka`, relying on `@Nullable`/javadoc convention (or platform types, if
called from Kotlin directly against `spring-kafka`'s Java classes):

```java
@Nullable Exception checkForErrors(ConsumerRecord<K, R> record); // must remember to null-check
```

`spring-kafka-kotlin`, real Kotlin nullability — the compiler enforces it:

```kotlin
fun setReplyErrorChecker(replyErrorChecker: (ConsumerRecord<*, *>) -> Exception?) // Exception? is a compile-time contract
```
