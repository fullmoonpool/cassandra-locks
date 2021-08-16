package com.fullmoonpool.cassandralocks

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mock
import org.mockito.Mockito.verify
import org.mockito.MockitoAnnotations
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.cassandra.core.cql.CqlTemplate
import java.util.concurrent.TimeUnit
import org.mockito.Mockito.`when` as mockWhen

class CassandraLockTest {

    private lateinit var closable: AutoCloseable

    @Mock
    private lateinit var cqlTemplate: CqlTemplate

    @BeforeEach
    fun setUp() {
        closable = MockitoAnnotations.openMocks(this)
    }

    @AfterEach
    fun tearDown() {
        closable.close()
    }

    @Test
    fun tryLock() {
        mockWhen(cqlTemplate.execute(any(Statement::class.java))).thenReturn(true)

        val lock = CassandraLock(cqlTemplate, "lock_id")

        val isAcquired = lock.tryLock(5, TimeUnit.SECONDS)
        assertEquals(true, isAcquired)

        verify(cqlTemplate).execute(any(Statement::class.java))
    }

    @Test
    fun `unlock with overtime`() {
        val lockId = "lock_id"

        val insertStatement: SimpleStatement = QueryBuilder
            .insertInto(CassandraLock.TABLE_NAME)
            .value(CassandraLock.COLUMN_NAME, QueryBuilder.bindMarker())
            .usingTtl(CassandraLock.TTL)
            .ifNotExists()
            .build(lockId)

        val deleteStatement: SimpleStatement = QueryBuilder
            .deleteFrom(CassandraLock.TABLE_NAME)
            .whereColumn(CassandraLock.COLUMN_NAME)
            .isEqualTo(QueryBuilder.bindMarker())
            .build(lockId)

        val timeout = 5L

        mockWhen(cqlTemplate.execute(any(Statement::class.java)))
            .then {
                logger.debug("Delay is over lock TTL")
                runBlocking { delay((timeout+ 1)*1000) }
                false
            }

        val lock = CassandraLock(cqlTemplate, lockId)

        val isAcquired = lock.tryLock(timeout, TimeUnit.SECONDS)
        assertEquals(false, isAcquired)

        lock.unlock()

        verify(cqlTemplate).execute(insertStatement)
        verify(cqlTemplate).execute(deleteStatement)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(CassandraLockTest::class.java)
    }
}