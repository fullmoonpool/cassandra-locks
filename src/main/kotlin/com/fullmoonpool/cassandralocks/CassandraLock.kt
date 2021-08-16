package com.fullmoonpool.cassandralocks

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable
import com.datastax.oss.driver.api.querybuilder.delete.Delete
import com.datastax.oss.driver.api.querybuilder.insert.Insert
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.data.cassandra.core.cql.CqlTemplate
import kotlin.jvm.Throws
import kotlin.system.measureTimeMillis
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime

class CassandraLock(
    private val cqlTemplate: CqlTemplate,
    private val lockId: String,
    private val ttl: Int = TTL,
    private val tableName: String = TABLE_NAME
) : SimpleCassandraLock {

    private var insert: Insert = QueryBuilder
        .insertInto(tableName)
        .value(COLUMN_NAME, bindMarker())
        .usingTtl(ttl)
        .ifNotExists()

    private var delete: Delete = QueryBuilder
        .deleteFrom(tableName)
        .whereColumn(COLUMN_NAME)
        .isEqualTo(bindMarker())

    private var create: CreateTable = createTable(tableName)
        .ifNotExists()
        .withPartitionKey(tableName, DataTypes.TEXT)

    @OptIn(ExperimentalTime::class)
    @Throws(DataAccessException::class)
    override fun tryLock(l: Long, durationUnit: DurationUnit): Boolean {
        val timeout = DurationUnit.MILLISECONDS.convert(l, durationUnit)
        val statement: SimpleStatement = insert.build(lockId)

        var totalTime = 0L
        var isAcquire = false
        var needRepeat = true

        while (needRepeat) {
            totalTime += measureTimeMillis {
                isAcquire = cqlTemplate.execute(statement)
            }
            logger.debug("${Thread.currentThread().name}: Locked value: $isAcquire")

            if (isAcquire) {
                logger.debug("${Thread.currentThread().name}: Lock was acquired")
                break
            }

            needRepeat = totalTime < timeout
        }

        return isAcquire
    }

    @Throws(DataAccessException::class)
    override fun unlock() {
        cqlTemplate.execute(delete.build(lockId))
        logger.debug("${Thread.currentThread().name}: Delete lock with id = $lockId")
    }

    @Throws(DataAccessException::class)
    override fun createIfNotExist(cqlTemplate: CqlTemplate) {
        cqlTemplate.execute(create.build())
        logger.debug("${Thread.currentThread().name}: Create lock table with name = $tableName")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(CassandraLock::class.java)
        const val TABLE_NAME = "distributed_locks"
        const val COLUMN_NAME = "id"
        const val TTL = 10
    }

}