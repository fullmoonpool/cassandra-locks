package com.fullmoonpool.cassandralocks

import org.springframework.data.cassandra.core.cql.CqlTemplate
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime

interface SimpleCassandraLock {

    @OptIn(ExperimentalTime::class)
    fun tryLock(l: Long, durationUnit: DurationUnit): Boolean

    fun unlock()

    fun createIfNotExist(cqlTemplate: CqlTemplate)

}