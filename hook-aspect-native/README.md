# hook-aspect-native

Source for `KafkaListenerCoroutineHook`/`KafkaListenerCoroutineHookAspect`, shared only across the
`spring-kafka-kotlin-*` modules on **spring-kafka 3.2 or later** (`3.2`, `3.3`, `4.0`, `4.1`) -- **not**
a dependency any of them declare via `project(":hook-aspect-native")`. Each applicable module's
`build.gradle.kts` instead adds this module's `src/main/kotlin` directly as an extra `kotlin.srcDir` on
its own `main` source set:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/hook-aspect-native/src/main/kotlin")
        }
    }
}
```

So files placed here get compiled independently into each of those 4 modules, against that module's own
`spring-kafka` version and classpath -- no shared jar, no runtime dependency on this module at all.

## Why only 3.2+

Suspend `@KafkaListener` methods aren't invokable at all on spring-kafka before 3.2, independent of this
library. `KotlinAwareInvocableHandlerMethod` -- spring-kafka's own suspend-aware method invocation,
`@since 3.2` -- is what makes the listener container capable of calling a suspend listener method in the
first place (resolving the compiler-synthesized trailing `Continuation` parameter and routing through
`CoroutinesUtils.invokeSuspendingFunction` instead of a plain reflective `Method.invoke`). Before 3.2, the
container's generic argument-resolution step fails outright for a suspend listener method -- no
`HandlerMethodArgumentResolver` in the default chain claims a `Continuation`-typed parameter -- so the
Hook feature would be silently non-functional on `spring-kafka-kotlin-2.8`/`2.9`/`3.0`/`3.1` even though
the code itself would compile fine there. See [IMPLEMENTATION.md](../IMPLEMENTATION.md) for the full
mechanism.

Suspend `@KafkaListener` support was backported to pre-3.2 spring-kafka this way -- see
`hook-aspect-compat`, which supplies its own custom `MessageHandlerMethodFactory` the same way
spring-kafka's own `KafkaMessageHandlerMethodFactory` does internally.
