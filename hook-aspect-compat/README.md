# hook-aspect-compat

Backport of suspend `@KafkaListener` support to spring-kafka before 3.2, shared only across
`spring-kafka-kotlin-2.8`, `2.9`, `3.0`, and `3.1` -- **not** a dependency any of them declare via
`project(":hook-aspect-compat")`. Each applicable module's `build.gradle.kts` instead adds this module's
`src/main/kotlin` directly as an extra `kotlin.srcDir` on its own `main` source set:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/hook-aspect-compat/src/main/kotlin")
        }
    }
}
```

So files placed here get compiled independently into each of those 4 modules, against that module's own
`spring-kafka` version and classpath -- no shared jar, no runtime dependency on this module at all.

Public API (`KafkaListenerCoroutineHook`, `KafkaListenerInvocation`, `KafkaListenerCoroutineHookAspect`)
is identical to `hook-aspect-native`'s. The internal invocation mechanism is deliberately different.

## Why this is needed at all

See [hook-aspect-native/README.md](../hook-aspect-native/README.md) for why `KafkaListenerCoroutineHook`
doesn't just work on spring-kafka before 3.2: the listener container's generic argument-resolution step
has no `HandlerMethodArgumentResolver` that recognizes the compiler-generated trailing `Continuation`
parameter every suspend function actually has, and fails before the method is ever invoked -- independent
of anything this library does.

`KafkaListenerCoroutineMessageHandlerMethodFactory` fixes that one step, by adding a resolver that claims
`Continuation`-typed parameters with an inert placeholder. It must be registered as a Spring bean visible
to `KafkaListenerAnnotationBeanPostProcessor` -- an explicit setup step 3.2+ doesn't need, since
spring-kafka ships the equivalent itself from that version on:

```kotlin
@Bean
fun messageHandlerMethodFactory(): MessageHandlerMethodFactory = KafkaListenerCoroutineMessageHandlerMethodFactory()
```

Confirmed directly, not assumed: a real test (`KafkaListenerCoroutineMessageHandlerMethodFactoryTest`)
shows a plain `DefaultMessageHandlerMethodFactory` actually fails invoking a suspend method
(`MessageConversionException`, from `PayloadMethodArgumentResolver` trying to convert the message payload
into a `Continuation` since nothing else claims that parameter first), and that this factory fixes it.

## Why the Aspect's own invocation mechanism differs from `hook-aspect-native`'s

`hook-aspect-native`'s Aspect hijacks the real `Continuation`, launches an async coroutine, and returns
`COROUTINE_SUSPENDED` -- relying on Spring Framework's own suspend-aware AOP proxy bridging
(`org.springframework.aop.framework.CoroutinesUtils`) to correctly propagate that back to the real caller.
That's a *Spring Framework* feature (not spring-kafka's), and checking each version's real `spring-aop`
sources directly confirms it doesn't exist on the Spring Framework versions spring-kafka 2.8/2.9/3.0
depend on (5.3.24, 5.3.29, 6.0.18) -- only spring-kafka 3.1's Spring Framework version (6.1.9) actually
has it.

So this module's Aspect never relies on it, uniformly across all 4 versions it supports: `hook` and
`processMessage` run synchronously inside `kotlinx.coroutines.runBlocking`, and `aroundKafkaListener`
returns the real computed result (or throws the real exception) directly -- never `COROUTINE_SUSPENDED`.
A suspend function completing synchronously is a normal, legal outcome of the suspend calling convention,
so this works correctly regardless of the underlying AOP proxy layer's own suspend-awareness. It's also
not a real behavioral downgrade for this use case: spring-kafka's listener container is a traditional
poll-based, single-thread-per-partition consumer loop, so the container thread is already fully occupied
for the whole listener invocation either way.

Plain `runBlocking`, not `runBlocking(Dispatchers.Unconfined)`, matters here specifically: `runBlocking`'s
own event-loop dispatcher guarantees the call resumes/returns on the *same thread that called it*, even
if `hook` relocates `processMessage` onto a different dispatcher internally (e.g.
`withContext(Dispatchers.IO) { processMessage() }`) -- `Dispatchers.Unconfined` would not guarantee this,
and returning on the wrong thread would violate ordinary synchronous method-call semantics for whatever
called `aroundKafkaListener`. Verified directly with a test that relocates `processMessage` onto a named
single-thread dispatcher and confirms the overall call still returns on the original calling thread.

One further consequence: unlike `hook-aspect-native`'s Aspect, this one has nothing running after
`aroundKafkaListener` returns -- there's no in-flight work a `DisposableBean.destroy()` would need to
cancel, so this class doesn't implement `DisposableBean`. That's a deliberate difference in an internal
lifecycle detail, not a gap in the public contract.
