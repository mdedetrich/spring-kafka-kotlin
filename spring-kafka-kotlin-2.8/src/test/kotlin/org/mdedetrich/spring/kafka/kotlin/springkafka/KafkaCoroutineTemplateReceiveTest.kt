package org.mdedetrich.spring.kafka.kotlin.springkafka

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

// A KafkaTemplate whose receive(...) doesn't touch a real Consumer/broker at all -- it just records
// which thread it ran on and returns a canned record, so these tests can assert on dispatcher
// placement without needing an embedded or reachable broker.
private class ThreadRecordingKafkaTemplate(
    producerFactory: ProducerFactory<String, String>,
) : KafkaTemplate<String, String>(producerFactory) {
    @Volatile
    var lastReceiveThreadName: String? = null

    override fun receive(
        topic: String,
        partition: Int,
        offset: Long,
        pollTimeout: Duration,
    ): ConsumerRecord<String, String> {
        lastReceiveThreadName = Thread.currentThread().name
        return ConsumerRecord(topic, partition, offset, "key-1", "payload")
    }
}

private fun mockProducerFactory() =
    object : ProducerFactory<String, String> {
        override fun createProducer() = MockProducer(true, StringSerializer(), StringSerializer())
    }

private suspend fun currentThreadName(): String = suspendCoroutine { continuation -> continuation.resume(Thread.currentThread().name) }

class KafkaCoroutineTemplateReceiveTest {
    // Plain fun + runBlocking, not `suspend fun`: JUnit 5.14.4 (this module's JVM 8 floor) doesn't
    // execute suspend @Test methods -- see KafkaCoroutineTemplateTest's identical note.
    @Test
    @Timeout(10)
    fun `receive with the default dispatcher runs off the calling coroutine's thread`() =
        runBlocking {
            val callingThreadName = currentThreadName()
            val delegate = ThreadRecordingKafkaTemplate(mockProducerFactory())
            val coroutineTemplate = KafkaCoroutineTemplate(delegate)

            val record = coroutineTemplate.receive("orders", 0, 0L)

            assertEquals("payload", record!!.value())
            assertNotEquals(callingThreadName, delegate.lastReceiveThreadName)
            assertEquals(true, delegate.lastReceiveThreadName?.contains("DefaultDispatcher"))
        }

    @Test
    @Timeout(10)
    fun `receive with a configured dispatcher runs on that dispatcher`() =
        runBlocking {
            val executor = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "receive-test-thread") }
            val dispatcher = executor.asCoroutineDispatcher()
            try {
                val delegate = ThreadRecordingKafkaTemplate(mockProducerFactory())
                val coroutineTemplate = KafkaCoroutineTemplate(delegate, blockingIODispatcher = dispatcher)

                coroutineTemplate.receive("orders", 0, 0L)

                assertEquals(true, delegate.lastReceiveThreadName?.startsWith("receive-test-thread"))
            } finally {
                executor.shutdown()
            }
        }

    @Test
    @Timeout(10)
    fun `receive with a null dispatcher runs directly on the calling coroutine's thread`() =
        runBlocking {
            val callingThreadName = currentThreadName()
            val delegate = ThreadRecordingKafkaTemplate(mockProducerFactory())
            val coroutineTemplate = KafkaCoroutineTemplate(delegate, blockingIODispatcher = null)

            coroutineTemplate.receive("orders", 0, 0L)

            assertEquals(callingThreadName, delegate.lastReceiveThreadName)
        }
}
