package com.squareup.sqldelight.drivers.native

import co.touchlab.sqliter.DatabaseConfiguration
import co.touchlab.sqliter.DatabaseConnection
import co.touchlab.sqliter.DatabaseManager
import co.touchlab.sqliter.Statement
import co.touchlab.sqliter.createDatabaseManager
import co.touchlab.stately.concurrency.AtomicBoolean
import co.touchlab.stately.concurrency.AtomicReference
import co.touchlab.stately.concurrency.ThreadLocalRef
import co.touchlab.stately.concurrency.value
import co.touchlab.stately.freeze
import com.squareup.sqldelight.Transacter
import com.squareup.sqldelight.db.SqlCursor
import com.squareup.sqldelight.db.SqlDriver
import com.squareup.sqldelight.db.SqlPreparedStatement

sealed class ConnectionWrapper : SqlDriver {
  internal abstract fun <R> accessConnection(
    useTransactionPool: Boolean,
    block: ThreadConnection.() -> R
  ): R

  final override fun execute(
    identifier: Int?,
    sql: String,
    parameters: Int,
    binders: (SqlPreparedStatement.() -> Unit)?
  ) {
    accessConnection(true) {
      val statement = getStatement(identifier, sql)
      if (binders != null) {
        try {
          SqliterStatement(statement).binders()
        } catch (t: Throwable) {
          endStatementAccess(identifier, statement)
          throw t
        }
      }

      statement.execute()
      endStatementAccess(identifier, statement)
    }
  }

  final override fun <R> executeQuery(
    identifier: Int?,
    sql: String,
    parameters: Int,
    binders: (SqlPreparedStatement.() -> Unit)?,
    block: (SqlCursor) -> R
  ): R {
    return accessConnection(false) {
      val statement = getStatement(identifier, sql)

      if (binders != null) {
        try {
          SqliterStatement(statement).binders()
        } catch (t: Throwable) {
          endStatementAccess(identifier, statement)
          throw t
        }
      }

      val cursor = statement.query()
      val wrappedCursor = SqliterSqlCursor(cursor)
      val result = block(wrappedCursor)

      endStatementAccess(identifier, statement)

      result
    }
  }
}

/**
 * Native driver implementation.
 *
 * The root SqlDriver creates 2 connections to the underlying database. One is used by
 * transactions and aligned with the thread performing the transaction. Multiple threads starting
 * transactions block and wait. The other connection does everything outside of a connection. The
 * goal is to be able to read while also writing. Future versions may create multiple query
 * connections.
 *
 * When a transaction is started, that thread is aligned with the transaction connection. Attempting
 * to start a transaction on another thread will block until the first finishes. Not ending
 * transactions is problematic, but it would be regardless.
 *
 * One implication to be aware of. You cannot operate on a single transaction from multiple threads.
 * However, it would be difficult to find a use case where this would be desirable or safe.
 *
 * To use SqlDelight during create/upgrade processes, you can alternatively wrap a real connection
 * with wrapConnection.
 *
 * SqlPreparedStatement instances also do not point to real resources until either execute or
 * executeQuery is called. The SqlPreparedStatement structure also maintains a thread-aligned
 * instance which accumulates bind calls. Those are replayed on a real SQLite statement instance
 * when execute or executeQuery is called. This avoids race conditions with bind calls.
 */
class NativeSqliteDriver(
  private val databaseManager: DatabaseManager
) : ConnectionWrapper(),
  SqlDriver {

  constructor(
    configuration: DatabaseConfiguration
  ) : this(
    databaseManager = createDatabaseManager(configuration)
  )

  constructor(
    schema: SqlDriver.Schema,
    name: String
  ) : this(
    configuration = DatabaseConfiguration(
      name = name,
      version = schema.version,
      create = { connection ->
        wrapConnection(connection) { schema.create(it) }
      },
      upgrade = { connection, oldVersion, newVersion ->
        wrapConnection(connection) { schema.migrate(it, oldVersion, newVersion) }
      }
    )
  )

  // Once a transaction is started and connection borrowed, it will be here, but only for that
  // thread
  private val borrowedConnectionThread =
    ThreadLocalRef<SinglePool<ThreadConnection>.Borrowed>()

  // Connection used by all operations not in a transaction
  internal val queryPool = SinglePool {
    ThreadConnection(databaseManager.createMultiThreadedConnection(), borrowedConnectionThread)
  }

  // Connection which can be borrowed by a thread, to ensure all transaction ops happen in the same
  // place. In WAL mode (default) reads can happen while this is also going on
  internal val transactionPool = SinglePool {
    ThreadConnection(databaseManager.createMultiThreadedConnection(), borrowedConnectionThread)
  }

  override fun currentTransaction(): Transacter.Transaction? {
    return borrowedConnectionThread.get()?.entry?.transaction?.value
  }

  override fun newTransaction(): Transacter.Transaction {
    val alreadyBorrowed = borrowedConnectionThread.get()
    return if (alreadyBorrowed == null) {
      val borrowed = transactionPool.borrowEntry()
      try {
        val trans = borrowed.entry.newTransaction()
        borrowedConnectionThread.value =
          borrowed.freeze() // Probably don't need to freeze, but revisit.
        trans
      } catch (e: Throwable) {
        // Unlock on failure.
        borrowed.release()
        throw e
      }
    } else {
      alreadyBorrowed.entry.newTransaction()
    }
  }

  /**
   * If we're in a transaction, then I have a connection. Otherwise use shared.
   */
  override fun <R> accessConnection(useTransactionPool: Boolean, block: ThreadConnection.() -> R): R {
    val mine = borrowedConnectionThread.get()
    return if (mine != null) {
      mine.entry.block()
    } else {
      if (useTransactionPool) {
        transactionPool.access { it.block() }
      } else {
        queryPool.access { it.block() }
      }
    }
  }

  override fun close() {
    transactionPool.access { it.close() }
    queryPool.access { it.close() }
  }
}

/**
 * Sqliter's DatabaseConfiguration takes lambda arguments for it's create and upgrade operations,
 * which each take a DatabaseConnection argument. Use wrapConnection to have SqlDelight access this
 * passed connection and avoid the pooling that the full SqlDriver instance performs.
 *
 * Note that queries created during this operation will be cleaned up. If holding onto a cursor from
 * a wrap call, it will no longer be viable.
 */
fun wrapConnection(
  connection: DatabaseConnection,
  block: (SqlDriver) -> Unit
) {
  val conn = SqliterWrappedConnection(ThreadConnection(connection, null))
  try {
    block(conn)
  } finally {
    conn.close()
  }
}

/**
 * SqlDriverConnection that wraps a Sqliter connection. Useful for migration tasks, or if you
 * don't want the polling.
 */
internal class SqliterWrappedConnection(
  private val threadConnection: ThreadConnection
) : ConnectionWrapper(),
  SqlDriver {
  override fun currentTransaction(): Transacter.Transaction? = threadConnection.transaction.value

  override fun newTransaction(): Transacter.Transaction = threadConnection.newTransaction()

  override fun <R> accessConnection(
    useTransactionPool: Boolean,
    block: ThreadConnection.() -> R
  ): R = threadConnection.block()

  override fun close() {
    threadConnection.cleanUp()
  }
}

/**
 * Wraps and manages a "real" database connection.
 *
 * SQLite statements are specific to connections, and must be finalized explicitly. Cursors are
 * backed by a statement resource, so we keep links to open cursors to allow us to close them out
 * properly in cases where the user does not.
 */
internal class ThreadConnection(
  private val connection: DatabaseConnection,
  private val borrowedConnectionThread: ThreadLocalRef<SinglePool<ThreadConnection>.Borrowed>?
) {
  internal val transaction: AtomicReference<Transacter.Transaction?> = AtomicReference(null)

  // A copy-on-write statement cache.
  // Mutations are expected to be rare anyway: once per statement per connection per application invocation.
  private val statementCache = AtomicReference(mapOf<Int, StatementEntry>().freeze())

  private class StatementEntry(val statement: Statement) {
    // Mutating a stately-collections [SharedHashMap] or the simple CoW cache we have is an order of
    // mangitude more expensive than stdlib [HashMap]. So instead of preventing incorrect reuse by
    // removing in-use statements from the hash map, let's track usage via an [AtomicBoolean]
    // embedded in each entry and avoid collection manipulation entirely.
    val isInUse = AtomicBoolean(false)
  }

  fun endStatementAccess(identifier: Int?, statement: Statement) {
    if (identifier != null) {
      val entry = statementCache.get()[identifier]

      if (entry != null && entry.statement == statement) {
        statement.resetStatement()
        val done = entry.isInUse.compareAndSet(expected = true, new = false)
        check(done) { "Entry should have been marked as in-use before, so as to be unmarked here." }
        return
      }
    }

    statement.finalizeStatement()
  }

  fun getStatement(identifier: Int?, sql: String): Statement {
    if (identifier != null) {
      val entry = statementCache.get()[identifier]

      if (entry == null) {
        val statement =  connection.createStatement(sql)

        statementCache.set(
          statementCache.get().toMutableMap()
            .apply {
              put(
                identifier,
                StatementEntry(statement)
                  .apply { isInUse.value = true }
              )
            }
            .freeze()
        )

        return statement
      }

      if (entry.isInUse.compareAndSet(expected = false, new = true)) {
        return entry.statement
      }
    }

    return connection.createStatement(sql)
  }

  fun newTransaction(): Transacter.Transaction {
    val enclosing = transaction.value

    // Create here, in case we bomb...
    if (enclosing == null) {
      connection.beginTransaction()
    }

    val trans = Transaction(enclosing).freeze()
    transaction.value = trans

    return trans
  }

  /**
   * This should only be called directly from wrapConnection. Clean resources without actually closing
   * the underlying connection.
   */
  internal fun cleanUp() {
    statementCache.get().forEach {
      it.value.statement.finalizeStatement()
    }
  }

  internal fun close() {
    cleanUp()
    connection.close()
  }

  private inner class Transaction(
    override val enclosingTransaction: Transacter.Transaction?
  ) : Transacter.Transaction() {
    override fun endTransaction(successful: Boolean) {
      // This stays here to avoid a race condition with multiple threads and transactions
      transaction.value = enclosingTransaction

      if (enclosingTransaction == null) {
        try {
          if (successful) {
            connection.setTransactionSuccessful()
          }

          connection.endTransaction()
        } finally {
          // Release if we have
          borrowedConnectionThread?.let {
            it.get()?.release()
            it.value = null
          }
        }
      }
    }
  }
}
