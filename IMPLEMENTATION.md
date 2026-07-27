# Implementation notes

Design rationale and internals for `spring-kafka-kotlin`. Not required reading to use the library —
see [README.md](README.md) for that. This document exists for contributors and the curious.

## Why `KafkaCoroutineOperations` instead of implementing `KafkaOperations`

`KafkaOperations`'s `send*`/`sendDefault` methods are declared to return
`CompletableFuture<SendResult<K, V>>` — that's baked into the interface itself, so implementing it
directly would mean either breaking the interface contract or exposing the exact `CompletableFuture`-
based API this project exists to avoid. `KafkaCoroutineOperations` mirrors `KafkaOperations`'s method
surface one-to-one, but with `suspend fun` in place of every `CompletableFuture`-returning method —
it's the coroutine-native contract `KafkaCoroutineTemplate` implements instead.

## Why blocking calls need a configurable dispatcher

Unlike `send`/`sendAndReceive`, which just `.await()` an already-async `CompletableFuture`,
`KafkaTemplate.receive` is a genuinely blocking call (a real `Consumer.poll` under the hood). Calling
it directly from a suspend function would block the calling coroutine's thread, so `receive` runs the
delegate call via a configurable `blockingIODispatcher: CoroutineDispatcher? = Dispatchers.IO` constructor
parameter, present on all four templates (`KafkaCoroutineTemplate`, `ReplyingKafkaCoroutineTemplate`,
`RoutingKafkaCoroutineTemplate`, `AggregatingReplyingKafkaCoroutineTemplate`). Passing `null` skips the
`withContext` hop entirely and calls the delegate directly on the calling coroutine's current
dispatcher — appropriate when that dispatcher is already safe for blocking calls, e.g. a
virtual-thread-per-task `Executor`-backed one.

`ReplyingKafkaCoroutineTemplate` and `AggregatingReplyingKafkaCoroutineTemplate`'s `waitForAssignment`
gets the same treatment for the same reason: it's a real `CountDownLatch.await()` under the hood, just
as blocking as `receive`'s `Consumer.poll()`.

Note that cancelling the calling coroutine while a blocking delegate call is suspended does **not**
interrupt it — `withContext` can't preempt a blocking JVM call it didn't itself create. Cancellation just
stops the *caller* from waiting on the result; the underlying call (`Consumer.poll()` or
`CountDownLatch.await()`) keeps running on the dispatcher's thread until it returns or times out on its
own.

## Why the repo is split one module per `spring-kafka` version

The wrapper's `KafkaCoroutineTemplate` delegates to `spring-kafka`'s `KafkaTemplate` and proxies its
public methods one-to-one. That delegation is the actual reason this repo is split one module per
`spring-kafka` major/minor line rather than one module supporting a version range: under semantic
versioning a minor bump is free to add new public methods to `KafkaTemplate`, and each of those needs a
corresponding proxied method here — there's no way to write a single delegate against a range of
`spring-kafka` minor versions and stay complete. (Separately, `spring-kafka` also isn't
binary-compatible across its major lines — 3.x requires Spring Framework 6 / Java 17, 2.x doesn't —
which would force a split on its own even ignoring the delegate surface.)

## Request/reply and routing templates

Besides `KafkaCoroutineTemplate`, the same delegate-and-coroutine-ify pattern applies to three more
`spring-kafka` classes:

- **`ReplyingKafkaCoroutineTemplate`** wraps `ReplyingKafkaTemplate`. Its `ParameterizedTypeReference<P>`-
  typed `sendAndReceive` overloads return a future (`RequestReplyTypedMessageFuture<K, V, P>`) declared
  as `CompletableFuture<Message<?>>` regardless of `P`, so a naive `.await()` alone would erase the
  result to `Message<*>` — but its `get()` is overridden covariantly to return `Message<P>`, and Kotlin
  resolves that override (not the synthetic erasure bridge Java generates for it) when called directly.
  So `.await()` is used only to suspend until completion/propagate failure, and the properly-typed
  result comes from a subsequent, non-blocking `get()`.
- **`RoutingKafkaCoroutineTemplate`** wraps `RoutingKafkaTemplate` (routes each send to one of several
  `ProducerFactory`s by topic regex). Fixed to `KafkaTemplate<Object, Object>` on the real class, so
  this wrapper is concrete/non-generic too; unsupported operations (`execute`, `flush`, etc.) just
  propagate `UnsupportedOperationException` unchanged.
- **`AggregatingReplyingKafkaCoroutineTemplate`** wraps `AggregatingReplyingKafkaTemplate` (aggregated
  `Collection<ConsumerRecord<K, R>>` reply instead of a single record).

All three have no Spring lifecycle of their own — the real delegate already implements
`SmartLifecycle`/`InitializingBean`/`DisposableBean` and must be registered as its own bean. Each reuses
`KafkaCoroutineTemplate` via interface delegation (`by KafkaCoroutineTemplate(delegate)`) for the base
send/receive/metrics surface rather than duplicating it by hand; `AggregatingReplyingKafkaCoroutineTemplate`
delegates to a freshly-constructed `KafkaCoroutineTemplate(delegate)` rather than nesting a
`ReplyingKafkaCoroutineTemplate(delegate)` (which would also work) to avoid constructing a second,
unreachable proxy purely to forward members already reachable on `delegate` directly.

## `sendAndReceiveDeferred`: exposing the send-only progress

The real `RequestReplyFuture`/`RequestReplyMessageFuture` expose `getSendFuture()` separately from the
future itself, so a caller can observe "was the request published" independent of "has the reply
arrived." The plain suspend `sendAndReceive` collapses both into a single await, losing that. Every
`sendAndReceive` overload therefore has a `sendAndReceiveDeferred` counterpart returning
`SendAndReceiveResult<K, V, R>` (`sendResult: Deferred<SendResult<K, V>>`, `reply: Deferred<R>`)
immediately, instead of suspending for the full round trip — matching the real API's shape, where
`sendAndReceive` also returns its future object synchronously.

Both `Deferred`s are built via `kotlinx-coroutines-jdk8`'s `CompletionStage<T>.asDeferred()`, not a
manually-launched coroutine — verified (via bytecode: `kotlinx.coroutines.future.FutureKt.asDeferred`)
to register a `CancelFutureOnCompletion` handler that calls `future.cancel(false)` when the `Deferred`
is cancelled, so cancelling `.reply` really does cancel the underlying `CompletableFuture`, not just
give up waiting on it locally.

For the `ParameterizedTypeReference<P>`-typed overload, `asDeferred()` alone can't be used for the reply
half: it infers its type from the future's *declared* type (`CompletableFuture<Message<?>>`), hitting
the same erasure problem `sendAndReceive`'s `.await()` does. Instead, a `CompletableDeferred<Message<P>>`
is completed manually from a `whenComplete` callback that calls the future's covariant `get()` once it's
known to be done — the same trick as the suspend overload, just delivered through a `Deferred` instead
of a direct suspend return.

`sendAndReceiveDeferred` is not guaranteed to return without throwing, despite "returns immediately"
being the whole point — verified via a real test (unreachable-broker metadata-fetch timeout): `Producer
.send()` (and so `KafkaTemplate.send()`/`ReplyingKafkaTemplate.sendAndReceive()`) can throw
*synchronously* for failures detected before the record is even buffered (e.g. no metadata for the
topic within `max.block.ms`), as opposed to failures detected after buffering (e.g. a broker rejection
delivered via the producer callback), which correctly fail only `sendResult` asynchronously instead.
Callers who want failure reported solely through `sendResult`/`reply` rather than a direct throw still
need to wrap the `sendAndReceiveDeferred` call itself in a `try`/`catch`.

## Reified `sendAndReceiveTyped`/`sendAndReceiveDeferredTyped`

Extension functions offering `inline fun <reified P> ...(message): Message<P>`, building the
`ParameterizedTypeReference<P>` from the reified type argument so callers don't write
`object : ParameterizedTypeReference<P>() {}` by hand.

These are **not** named `sendAndReceive`/`sendAndReceiveDeferred` (overloading the existing members):
Kotlin always prefers a member function over an extension of the same name once the member is
otherwise callable by its value-parameter shape, regardless of a differing type-parameter list — since
a single-`Message<*>`-parameter member already exists, an identically-named extension taking the same
parameter could never be selected, even with an explicit type argument at the call site. Distinct names
sidestep the shadowing rule entirely.

Also note Kotlin has no partial explicit-type-argument syntax: given `<reified P, K, V, R>`, a call site
can supply all four type arguments or none, never just `<String>` for `P` alone. `K`/`V`/`R` still infer
fine from the receiver either way, but `P` — being reified and otherwise unconstrained by any value
parameter — needs an explicit target type at the call site to infer from (e.g. a `val result: Message<Foo> = ...`
declaration) rather than an explicit `<Foo>` type argument.

## `KafkaListenerCoroutineHookAspect`: a `CoWebFilter`-style hook for suspend `@KafkaListener` methods

### The problem

The motivating use case: extract a correlation id from the incoming `ConsumerRecord`'s headers and make
it available to the listener body (and anything it suspends into) via a `CoroutineContext.Element` —
the `@KafkaListener` analogue of what `CoWebFilter` does for suspend WebFlux handlers.

`KafkaListenerCoroutineHook` receives a `processMessage: suspend () -> Any?` closure representing the
real listener body and decides how (and whether) to call it, typically
`withContext(...) { processMessage() }`. Because the hook controls the call itself, rather than just
contributing something for the Aspect to apply beforehand, it can also short-circuit without invoking
the listener, retry it, catch/translate exceptions, or run code both before *and* after it completes.

Separately: `spring-kafka` already detects suspend `@KafkaListener` methods itself
(`org.springframework.kafka.listener.adapter.KotlinAwareInvocableHandlerMethod.doInvoke`, verified via
bytecode), invoking them through `org.springframework.core.CoroutinesUtils.invokeSuspendingFunction`,
which always starts the coroutine on `Dispatchers.Unconfined` — there's a 4-arg overload of that method
accepting a custom `CoroutineContext`, but spring-kafka never calls it. There is no built-in hook to
install additional context around that invocation.

### Why AOP works here at all

The naive worry with intercepting a suspend function via `@Around` advice is the classic Kotlin/Java
interop trap: `ProceedingJoinPoint.proceed()` is a plain blocking call: for a genuinely suspending target
it can return the real result *or* the `COROUTINE_SUSPENDED` marker, and ordinary "before + after" advice
code that assumes `proceed()` fully completed the call would run its "after" logic at the wrong time.

This turned out not to be a problem here, but not for a subtle reason worth trusting blindly — it was
verified against a running Spring context (a throwaway `@Aspect` + suspend target with a real `delay()`
suspension point) before being relied on. Both `CglibAopProxy` and `JdkDynamicAopProxy` call
`KotlinDetector.isSuspendingFunction(method)` on the intercepted method themselves and, when true, bridge
the advice chain's result back into the original caller's real `Continuation` via a separate
`org.springframework.aop.framework.CoroutinesUtils` (a different class from the spring-core one used by
spring-kafka). Confirmed empirically: `ProceedingJoinPoint.getArgs()` for a suspend target still includes
the real, unmodified `Continuation` as its last element, unstripped by that bridging.

### Why this Aspect bypasses `proceed()` for the actual invocation

Given the `Continuation` is available intact, the robust design doesn't lean on exactly how Spring's own
bridging behaves for the wrapping part — it manually replicates the suspend calling convention instead,
which is easier to reason about and test in isolation:

1. Detect suspend via `KotlinDetector.isSuspendingFunction(method)`; non-suspend listeners just
   `proceed()` normally, untouched. Also `proceed()` immediately, with no reflection at all, if `hook`
   is exactly `KafkaListenerCoroutineHook.NONE` -- a known-safe fast path, since that specific instance
   is known in advance to just call `processMessage()` unchanged.
2. Classify the invocation (`KafkaListenerInvocation`) and build a `processMessage: suspend () -> Any?`
   closure that invokes the real method via Kotlin reflection
   (`method.kotlinFunction!!.callSuspend(bean, *businessArgs)`) -- not called yet, just captured.
3. Pull the real `Continuation` off the end of `joinPoint.args`.
4. Start a coroutine (via `kotlin.coroutines.startCoroutine`, not `kotlinx.coroutines.launch` -- see
   "Why `startCoroutine`, not `launch`" below) on `continuation.context.minusKey(Job) + Dispatchers
   .Unconfined + invocationJob` -- `invocationJob` a fresh `Job` parented to an aspect-owned
   `CoroutineScope(SupervisorJob())` so `destroy()` can still cancel it if still in flight when the
   Spring context closes, `Dispatchers.Unconfined` matching `spring-kafka`'s own default (the hook is
   free to move elsewhere itself via `withContext`) -- that calls `hook.hook(invocation, processMessage)`
   and resumes the original `Continuation` with whatever that call returns (or throws).
5. Return `kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED` immediately — the same signal a genuinely
   suspending call would produce, which the outer CGLIB/JDK proxy layer (from the previous section)
   already knows how to bridge back to the real caller.

Verified end-to-end against a running Spring context: the advice method returns immediately, the
relaunched coroutine runs with whatever the hook installed via `withContext` (correctly combined when
one hook nests another's), and the original caller receives the final result once the continuation
resumes.

### Why the relaunched coroutine builds on `continuation.context`, not just `Dispatchers.Unconfined`

An earlier version launched on plain `scope.launch(Dispatchers.Unconfined)` -- the hijacked
`Continuation`'s own `context` was read only to pull the `Continuation` itself off `joinPoint.args`,
never consulted otherwise. Confirmed via a real test (`withContext(CustomElement(...)) {
listener.suspendMethod(...) }` from the caller side, asserting what a hook sees via
`coroutineContext[CustomElement]`): any `CoroutineContext.Element` the caller already had installed was
silently invisible inside `hook`/`processMessage` -- they only ever saw whatever the aspect's own bare
`scope` provided, plus whatever the hook itself installed via `withContext`. Fixed by building the
invocation's context from `continuation.context.minusKey(Job) + Dispatchers.Unconfined + invocationJob`
instead: the caller's own context elements (e.g. a tracing/MDC `ThreadContextElement`) now propagate, the
caller's own `Job` is stripped and replaced with a fresh `invocationJob` (a child of the aspect-owned
`scope`'s `SupervisorJob`, so `destroy()` still cancels it if still in flight), and `Dispatchers.Unconfined`
is added last so it still wins over whatever dispatcher the caller's context happened to carry.

### Why `startCoroutine`, not `launch`

`kotlinx.coroutines.launch` allocates a full `StandaloneCoroutine` -- structured-concurrency bookkeeping
(child registration, completion propagation, exception handling) this single fire-and-forget invocation
never uses: there are no children of its own, no result exposed via the `Job`, nothing external awaiting
its completion. `kotlin.coroutines.startCoroutine` (stdlib, not `kotlinx.coroutines`) starts the same
suspend block against a plain `Continuation` completion callback, with no `Job`/`CoroutineScope` of its
own -- confirmed via a dedicated benchmark to be meaningfully cheaper per invocation (9-13% faster on the
full cache-hit path across all three `KafkaListenerInvocation` shapes).

The real risk this could have introduced: `destroy()`'s cancellation of in-flight invocations depended on
every invocation being a child of `scope`'s `SupervisorJob`, which `launch` provided automatically.
`startCoroutine` doesn't hand back a `Job` at all, so a fresh `invocationJob = Job(parent =
scope.coroutineContext[Job])` is constructed manually and included in the context passed to
`startCoroutine` -- cooperative cancellation checks at suspension points key off whichever `Job` is
present in the *coroutine's own context*, not on how the coroutine was started, so this is a real
`Job`, not a cosmetic one. Verified end-to-end with a real test (a hook stuck in `awaitCancellation()`,
closing the owning `ApplicationContext` mid-invocation): the hook genuinely receives
`CancellationException`, and so does the original caller. The remaining cost of constructing and
completing that `Job` per invocation is real (it eats back some of `startCoroutine`'s own saving), but the
net effect is still faster than the original `launch`-based version, with no loss of the cancellation
guarantee.

### Why `KafkaListenerInvocation` is a sealed class, and classifies from declared parameter types

`@KafkaListener` methods can be declared in several genuinely different shapes: a single
`ConsumerRecord<K, V>` parameter, a batch `List<ConsumerRecord<K, V>>` parameter, or individual
`@Payload`/`@Header` parameters with no `ConsumerRecord` at all. A nullable-field flattened shape can
express "single record, if present" but has no way to represent "this is specifically a batch listener"
as its own case. `KafkaListenerInvocation` is `sealed` with one subtype per shape (`SingleRecord`,
`BatchRecords`, `IndividualParameters`) so a hook can `when`-match exhaustively -- the compiler enforces
that every case is handled.

Classification happens from the method's *declared* parameter types (`Method.genericParameterTypes`),
not the runtime argument values: an empty `List<ConsumerRecord<K, V>>` and an empty `List<String>` are
indistinguishable at runtime (type erasure), but the declared generic parameter type always says which
one it is. This needs one non-obvious extra case, found by a real test failure rather than assumed:
Kotlin's read-only `List<T>` is declaration-site covariant, so a listener declaring
`records: List<ConsumerRecord<K, V>>` compiles to Java's `List<? extends ConsumerRecord<K, V>>` -- the
element type reflection sees is a `WildcardType` (from `actualTypeArguments`), not a bare
`Class`/`ParameterizedType`, and needs its `upperBounds` resolved one level further to reach the real
`ConsumerRecord` class.

### Why `IndividualParameters.payload`/`headers` are as far as this can be properly typed

Unlike `SingleRecord`/`BatchRecords`, `IndividualParameters` has no single fixed shape -- which
`@Header`s (if any) a listener declares, and what type it declares `@Payload` as, is chosen per listener
method, not knowable in advance. Named fields like `value: String`/`key: String`/`partition: Int` for
specific, hard-coded header names would be presumptuous (most `IndividualParameters` listeners don't
declare all of those) and incomplete (custom header names can't be anticipated at all).

What *is* available regardless of which specific parameters a listener declares: each `@Header`-annotated
parameter carries its header name as annotation metadata (`Method.parameterAnnotations`), inspectable via
reflection independent of what that name actually is. `classifyInvocation` reads
`method.parameterAnnotations` (index-aligned with `businessArgs`, the same alignment relied on for the
record-shape/`Acknowledgment`/`Consumer` extraction) and builds `headers: Map<String, Any?>` keyed by
each `@Header`'s `value` (aliased to `name` on
`org.springframework.messaging.handler.annotation.Header`), plus `payload: Any?` for whichever parameter
(if any) carries `@Payload`. This turns `invocation.headers["correlationId"]` (named, but still `Any?`
per entry -- the type is whatever that specific listener declared) into the honest ceiling here, rather
than pretending a handful of hard-coded named properties could cover every real `IndividualParameters`
listener shape.
