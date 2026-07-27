package org.mdedetrich.spring.kafka.kotlin.benchmarks

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mdedetrich.spring.kafka.kotlin.springkafka.aop.KafkaListenerCoroutineHook
import org.mdedetrich.spring.kafka.kotlin.springkafka.aop.KafkaListenerCoroutineHookAspect
import org.mdedetrich.spring.kafka.kotlin.springkafka.aop.KafkaListenerInvocation
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.EnableAspectJAutoProxy
import org.springframework.kafka.annotation.KafkaListener
import java.util.concurrent.TimeUnit

// Same shape as the hook-aspect-native benchmark's own BenchmarkListener -- no real Kafka broker/listener
// container, @KafkaListener is inert metadata matched only by this Aspect's own AspectJ pointcut.
open class BenchmarkListener {
    @KafkaListener(topics = ["orders"], groupId = "benchmark", autoStartup = "false")
    open suspend fun processMessage(record: ConsumerRecord<String, String>): String = "processed:${record.value()}"
}

@Configuration
@EnableAspectJAutoProxy
open class NoOpHookConfig {
    @Bean
    open fun benchmarkListener() = BenchmarkListener()

    @Bean
    open fun hook(): KafkaListenerCoroutineHook =
        object : KafkaListenerCoroutineHook() {
            override suspend fun hook(
                invocation: KafkaListenerInvocation,
                processMessage: suspend () -> Any?,
            ): Any? = processMessage()
        }

    @Bean
    open fun kafkaListenerCoroutineHookAspect(hook: KafkaListenerCoroutineHook) = KafkaListenerCoroutineHookAspect(hook)
}

// No @EnableAspectJAutoProxy, no Aspect bean at all -- benchmarkListener() comes back unproxied, a direct
// suspend call baseline with none of this backport's machinery in the picture.
@Configuration
open class PlainConfig {
    @Bean
    open fun benchmarkListener() = BenchmarkListener()
}

// The Aspect is present and the bean is CGLIB-proxied, but hook is exactly NONE -- isolates CGLIB proxy
// dispatch cost from this Aspect's own reflection/classification/invocation-bridge cost.
@Configuration
@EnableAspectJAutoProxy
open class NoneHookConfig {
    @Bean
    open fun benchmarkListener() = BenchmarkListener()

    @Bean
    open fun hook(): KafkaListenerCoroutineHook = KafkaListenerCoroutineHook.NONE

    @Bean
    open fun kafkaListenerCoroutineHookAspect(hook: KafkaListenerCoroutineHook) = KafkaListenerCoroutineHookAspect(hook)
}

/**
 * Benchmarks [KafkaListenerCoroutineHookAspect]'s own invocation-bridge mechanism specifically -- the
 * [kotlin.coroutines.startCoroutine] + [java.util.concurrent.CountDownLatch] design measured against the three same baselines the
 * `hook-aspect-native` benchmark uses, to see whether that design is actually cheaper than the plain
 * [runBlocking] it replaced (see `hook-aspect-compat/README.md`).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
open class KafkaListenerCoroutineHookAspectBenchmark {
    private lateinit var hookAspectContext: AnnotationConfigApplicationContext
    private lateinit var hookAspectListener: BenchmarkListener
    private lateinit var plainContext: AnnotationConfigApplicationContext
    private lateinit var plainListener: BenchmarkListener
    private lateinit var noneHookContext: AnnotationConfigApplicationContext
    private lateinit var noneHookListener: BenchmarkListener
    private lateinit var record: ConsumerRecord<String, String>

    @Setup
    fun setup() {
        hookAspectContext = AnnotationConfigApplicationContext(NoOpHookConfig::class.java)
        hookAspectListener = hookAspectContext.getBean(BenchmarkListener::class.java)
        plainContext = AnnotationConfigApplicationContext(PlainConfig::class.java)
        plainListener = plainContext.getBean(BenchmarkListener::class.java)
        noneHookContext = AnnotationConfigApplicationContext(NoneHookConfig::class.java)
        noneHookListener = noneHookContext.getBean(BenchmarkListener::class.java)
        record = ConsumerRecord("orders", 0, 0L, "key-1", "value-1")
    }

    @TearDown
    fun tearDown() {
        hookAspectContext.close()
        plainContext.close()
        noneHookContext.close()
    }

    // kotlinx-benchmark's JMH-based generator doesn't support suspend @Benchmark methods -- see
    // hook-aspect-native benchmark's own identical comment. runBlocking here is purely this benchmark's own
    // bridge from a plain JMH method into a suspend call site; unrelated to the Aspect's own internal
    // mechanism being measured.
    @Benchmark
    fun invokeThroughHookAspect(): String = runBlocking { hookAspectListener.processMessage(record) }

    @Benchmark
    fun invokeWithoutHookAspect(): String = runBlocking { plainListener.processMessage(record) }

    @Benchmark
    fun invokeWithNoneHook(): String = runBlocking { noneHookListener.processMessage(record) }
}
