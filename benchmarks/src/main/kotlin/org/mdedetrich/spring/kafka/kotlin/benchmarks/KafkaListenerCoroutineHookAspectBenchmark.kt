package org.mdedetrich.spring.kafka.kotlin.benchmarks

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mdedetrich.spring.kafka.kotlin.aop.KafkaListenerCoroutineHook
import org.mdedetrich.spring.kafka.kotlin.aop.KafkaListenerCoroutineHookAspect
import org.mdedetrich.spring.kafka.kotlin.aop.KafkaListenerInvocation
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
import org.springframework.messaging.handler.annotation.Payload
import java.util.concurrent.TimeUnit

// No real Kafka broker/listener container here (same as the library's own tests) -- @KafkaListener is
// inert metadata, matched only by KafkaListenerCoroutineHookAspect's own AspectJ pointcut. Called directly
// like any other proxied Spring bean method. One method per KafkaListenerInvocation shape, so the
// classification cost (declared-parameter-type walk in listenerMethodMetadata, done once and cached per
// Method, not per message) is exercised for all three, not just SingleRecord.
open class BenchmarkListener {
    @KafkaListener(topics = ["orders"], groupId = "benchmark", autoStartup = "false")
    open suspend fun processMessage(record: ConsumerRecord<String, String>): String = "processed:${record.value()}"

    // Trivial body matching the other two shapes' effort level (a single string template, no per-element
    // work) -- joinToString-ing every record would add real work of its own, unrelated to AOP/Aspect
    // overhead, and skew the "no proxy" baseline higher than the other two shapes' for no relevant reason.
    @KafkaListener(topics = ["orders-batch"], groupId = "benchmark", autoStartup = "false")
    open suspend fun processBatch(records: List<ConsumerRecord<String, String>>): String = "processed:${records.size}"

    @KafkaListener(topics = ["orders-payload"], groupId = "benchmark", autoStartup = "false")
    open suspend fun processPayload(
        @Payload value: String,
    ): String = "processed:$value"
}

@Configuration
@EnableAspectJAutoProxy
open class NoOpHookConfig {
    @Bean
    open fun benchmarkListener() = BenchmarkListener()

    // A hook that does nothing extra -- isolates the Aspect's own reflection/classification/relaunch
    // overhead (the code path listenerMethodMetadata's cache targets) from any work a real hook would add.
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

// No @EnableAspectJAutoProxy, no KafkaListenerCoroutineHookAspect bean at all -- benchmarkListener() comes
// back as a plain, unproxied object, so calling it is just a direct suspend call with none of this
// library's machinery in the picture. The baseline invokeThroughHookAspect is measured against.
@Configuration
open class PlainConfig {
    @Bean
    open fun benchmarkListener() = BenchmarkListener()
}

// The Aspect is present and the bean is CGLIB-proxied, but `hook` is exactly NONE -- aroundKafkaListener's
// very first check (`hook === KafkaListenerCoroutineHook.NONE`) short-circuits to plain `proceed()`, with
// no reflection, no classification, no coroutine relaunch at all. Isolates CGLIB proxy dispatch (present
// here, absent in PlainConfig) from this Aspect's own reflection/relaunch machinery (present in
// NoOpHookConfig, absent here).
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
 * Benchmarks [KafkaListenerCoroutineHookAspect]'s steady-state (cache-hit) invocation path -- every call
 * after the first exercises `listenerMethodMetadata`'s cache lookup and `classifyInvocation` rather than
 * re-deriving the method's shape via reflection, so this directly measures that caching's effect -- against
 * two baselines: a plain, unintercepted suspend call (no CGLIB proxy at all), and a CGLIB-proxied call
 * through the Aspect with `hook = NONE` (proxy present, but the Aspect's own machinery never runs). The
 * gap between the two baselines is CGLIB/Spring AOP's own dispatch cost; the gap from NONE up to the
 * full hook is this Aspect's own reflection/relaunch/classification cost. Repeated for all three
 * [org.mdedetrich.spring.kafka.kotlin.aop.KafkaListenerInvocation] shapes ([KafkaListenerInvocation.SingleRecord],
 * [KafkaListenerInvocation.BatchRecords], [KafkaListenerInvocation.IndividualParameters]), since classification/construction differs per shape.
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
    private lateinit var records: List<ConsumerRecord<String, String>>

    @Setup
    fun setup() {
        hookAspectContext = AnnotationConfigApplicationContext(NoOpHookConfig::class.java)
        hookAspectListener = hookAspectContext.getBean(BenchmarkListener::class.java)
        plainContext = AnnotationConfigApplicationContext(PlainConfig::class.java)
        plainListener = plainContext.getBean(BenchmarkListener::class.java)
        noneHookContext = AnnotationConfigApplicationContext(NoneHookConfig::class.java)
        noneHookListener = noneHookContext.getBean(BenchmarkListener::class.java)
        record = ConsumerRecord("orders", 0, 0L, "key-1", "value-1")
        records =
            listOf(
                ConsumerRecord("orders-batch", 0, 0L, "key-1", "value-1"),
                ConsumerRecord("orders-batch", 0, 1L, "key-2", "value-2"),
            )
    }

    @TearDown
    fun tearDown() {
        hookAspectContext.close()
        plainContext.close()
        noneHookContext.close()
    }

    // kotlinx-benchmark's JMH-based generator doesn't support suspend @Benchmark methods -- it only sees
    // the compiled signature's extra Continuation parameter, which JMH's own generator rejects as an
    // unrecognized argument (confirmed directly: "Method parameters should be either @State classes or
    // one of special JMH classes"). runBlocking is the wrapper.
    @Benchmark
    fun singleRecordThroughHookAspect(): String = runBlocking { hookAspectListener.processMessage(record) }

    @Benchmark
    fun singleRecordWithoutHookAspect(): String = runBlocking { plainListener.processMessage(record) }

    @Benchmark
    fun singleRecordWithNoneHook(): String = runBlocking { noneHookListener.processMessage(record) }

    @Benchmark
    fun batchRecordsThroughHookAspect(): String = runBlocking { hookAspectListener.processBatch(records) }

    @Benchmark
    fun batchRecordsWithoutHookAspect(): String = runBlocking { plainListener.processBatch(records) }

    @Benchmark
    fun batchRecordsWithNoneHook(): String = runBlocking { noneHookListener.processBatch(records) }

    @Benchmark
    fun individualParametersThroughHookAspect(): String = runBlocking { hookAspectListener.processPayload("value-1") }

    @Benchmark
    fun individualParametersWithoutHookAspect(): String = runBlocking { plainListener.processPayload("value-1") }

    @Benchmark
    fun individualParametersWithNoneHook(): String = runBlocking { noneHookListener.processPayload("value-1") }
}
