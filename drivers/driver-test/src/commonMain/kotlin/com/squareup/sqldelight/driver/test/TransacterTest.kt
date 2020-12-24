package com.squareup.sqldelight.driver.test

import com.squareup.sqldelight.TransacterImpl
import com.squareup.sqldelight.db.SqlDriver
import com.squareup.sqldelight.db.SqlDriver.Schema
import com.squareup.sqldelight.db.use
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

abstract class TransacterTest {
  protected lateinit var transacter: TransacterImpl
  private lateinit var driver: SqlDriver

  abstract fun setupDatabase(schema: Schema): SqlDriver

  @BeforeTest fun setup() {
    val driver = setupDatabase(object : Schema {
      override val version = 1
      override fun create(driver: SqlDriver) {}
      override fun migrate(
        driver: SqlDriver,
        oldVersion: Int,
        newVersion: Int
      ) {
      }
    })
    transacter = object : TransacterImpl(driver) {}
    this.driver = driver
  }

  @AfterTest fun teardown() {
    driver.close()
  }

  @Test fun `afterCommit runs after transaction commits`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)
    }

    assertEquals(1, counter)
  }

  @Test fun `afterCommit does not run after transaction rollbacks`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)
      rollback()
    }

    assertEquals(0, counter)
  }

  @Test fun `afterCommit runs after enclosing transaction commits`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
        assertEquals(0, counter)
      }

      assertEquals(0, counter)
    }

    assertEquals(2, counter)
  }

  @Test fun `afterCommit does not run in nested transaction when enclosing rolls back`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
      }

      rollback()
    }

    assertEquals(0, counter)
  }

  @Test fun `afterCommit does not run in nested transaction when nested rolls back`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
        rollback()
      }

      throw AssertionError()
    }

    assertEquals(0, counter)
  }

  @Test fun `afterRollback no-ops if the transaction never rolls back`() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
    }

    assertEquals(0, counter)
  }

  @Test fun `afterRollback runs after a rollback occurs`() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
      rollback()
    }

    assertEquals(1, counter)
  }

  @Test fun `afterRollback runs after an inner transaction rolls back`() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
      transaction {
        rollback()
      }
      throw AssertionError()
    }

    assertEquals(1, counter)
  }

  @Test fun `afterRollback runs in an inner transaction when the outer transaction rolls back`() {
    var counter = 0
    transacter.transaction {
      transaction {
        afterRollback { counter++ }
      }
      rollback()
    }

    assertEquals(1, counter)
  }

  @Test fun `transactions close themselves out properly`() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
    }

    transacter.transaction {
      afterCommit { counter++ }
    }

    assertEquals(2, counter)
  }

  @Test fun `setting no enclosing fails if there is a currently running transaction`() {
    transacter.transaction(noEnclosing = true) {
      try {
        transacter.transaction(noEnclosing = true) {
          throw AssertionError()
        }
        throw AssertionError()
      } catch (e: IllegalStateException) {
        // Expected error.
      }
    }
  }

  @Test
  fun `An exception thrown in postRollback function is combined with the exception in the main body`() {
    class ExceptionA : RuntimeException()
    class ExceptionB : RuntimeException()
    try {
      transacter.transaction {
        afterRollback {
          throw ExceptionA()
        }
        throw ExceptionB()
      }
      fail("Should have thrown!")
    } catch (e: Throwable) {
      assertTrue("Exception thrown in body not in message($e)") { e.toString().contains("ExceptionA") }
      assertTrue("Exception thrown in rollback not in message($e)") { e.toString().contains("ExceptionB") }
    }
  }

  @Test
  fun `we can return a value from a transaction`() {
    val result: String = transacter.transactionWithResult {
      return@transactionWithResult "sup"
    }

    assertEquals(result, "sup")
  }

  @Test
  fun `we can rollback with value from a transaction`() {
    val result: String = transacter.transactionWithResult {
      rollback("rollback")
      return@transactionWithResult "sup"
    }

    assertEquals(result, "rollback")
  }

  @Test fun `write transaction cannot be enclosed by a read-only transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.read { transacter.write {} }
    }
    assertFailsWith<IllegalStateException> {
      transacter.readWithResult { transacter.writeWithResult {} }
    }
  }

  @Test fun `write transaction cannot be enclosed by a read-only transaction nested in a write transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.write { transacter.read { transacter.write {} } }
    }
    assertFailsWith<IllegalStateException> {
      transacter.writeWithResult { transacter.readWithResult { transacter.writeWithResult {} } }
    }
  }

  @Test fun `write transaction cannot be enclosed by a read-only transaction nested in a read-only transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.read { transacter.read { transacter.write {} } }
    }
    assertFailsWith<IllegalStateException> {
      transacter.readWithResult { transacter.readWithResult { transacter.writeWithResult {} } }
    }
  }

  @Test fun `reads are allowed in a read-only transaction`() {
    transacter.read { performRead() }
    val done = transacter.readWithResult<Boolean> { performRead() }
    assertTrue(done)
  }

  @Test fun `reads are allowed in a write transaction`() {
    transacter.write { performRead() }
    val done = transacter.writeWithResult<Boolean> { performRead() }
    assertTrue(done)
  }

  @Test fun `reads are allowed in a read-only transaction nested in a read-only transaction`() {
    transacter.read { transacter.read { performRead() } }
    val done = transacter.readWithResult<Boolean> {
      transacter.readWithResult { performRead() }
    }
    assertTrue(done)
  }

  @Test fun `reads are allowed in a read-only transaction nested in a write transaction`() {
    transacter.write { transacter.read { performRead() } }
    val done = transacter.writeWithResult<Boolean> { transacter.readWithResult { performRead() } }
    assertTrue(done)
  }

  @Test fun `reads are allowed in a write transaction nested in a write transaction`() {
    transacter.write { transacter.write { performRead() } }
    val done = transacter.writeWithResult<Boolean> { transacter.writeWithResult { performRead() } }
    assertTrue(done)
  }

  @Test fun `writes are prohibited in a read-only transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.read {
        performWrite()
      }
    }
    assertFailsWith<IllegalStateException> {
      transacter.readWithResult<Unit> {
        performWrite()
      }
    }
  }

  @Test fun `writes are prohibited in a read-only transaction nested in a write transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.write {
        transacter.read {
          performWrite()
        }
      }
    }
    assertFailsWith<IllegalStateException> {
      transacter.writeWithResult {
        transacter.readWithResult {
          performWrite()
        }
      }
    }
  }

  @Test fun `writes are prohibited in a read-only transaction nested in a read-only transaction`() {
    assertFailsWith<IllegalStateException> {
      transacter.read {
        transacter.read {
          performWrite()
        }
      }
    }
    assertFailsWith<IllegalStateException> {
      transacter.readWithResult {
        transacter.readWithResult {
          performWrite()
        }
      }
    }
  }

  private fun performRead(): Boolean {
    return driver.executeQuery(null, "PRAGMA user_version", 0, null)
      .use { it.next() }
      .apply { check(this) }
  }

  private fun performWrite() = driver.execute(null, "PRAGMA user_version = 0", 0, null)
}
