package org.mdedetrich.spring.kafka.kotlin

import org.springframework.beans.factory.annotation.Qualifier

/**
 * Qualifier for the optional [kotlinx.coroutines.CoroutineDispatcher] bean
 * [KafkaCoroutineTemplateAutoConfiguration]/[ReplyingKafkaCoroutineTemplateAutoConfiguration]/
 * [RoutingKafkaCoroutineTemplateAutoConfiguration]/[AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration]
 * pass through as `blockingIODispatcher`. Scoped to this specific injection point (rather than matching any
 * [kotlinx.coroutines.CoroutineDispatcher] bean in the context) since a `CoroutineDispatcher` isn't a
 * single canonical app-wide bean the way e.g. `TaskExecutor` is -- an unrelated `CoroutineDispatcher` bean
 * for some other feature should not be picked up here.
 */
@Target(AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FUNCTION, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
public annotation class BlockingIODispatcher
