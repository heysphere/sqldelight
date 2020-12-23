/*
 * Copyright (C) 2018 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.sqldelight

import com.squareup.sqldelight.db.SqlCursor
import com.squareup.sqldelight.db.SqlDriver
import com.squareup.sqldelight.db.use
import com.squareup.sqldelight.internal.QueryLock
import com.squareup.sqldelight.internal.sharedSet
import com.squareup.sqldelight.internal.withLock

/**
 * A listenable, typed query generated by SQLDelight.
 *
 * @param RowType the type that this query can map its result set to.
 */
@Suppress("FunctionName") // Emulating a constructor.
fun <RowType : Any> Query(
  identifier: Int,
  queries: MutableList<Query<*>>,
  driver: SqlDriver,
  query: String,
  mapper: (SqlCursor) -> RowType
): Query<RowType> {
  return Query(identifier, queries, driver, "unknown", "unknown", query, mapper)
}

/**
 * A listenable, typed query generated by SQLDelight.
 *
 * @param RowType the type that this query can map its result set to.
 */
@Suppress("FunctionName") // Emulating a constructor.
fun <RowType : Any> Query(
  identifier: Int,
  queries: MutableList<Query<*>>,
  driver: SqlDriver,
  fileName: String,
  label: String,
  query: String,
  mapper: (SqlCursor) -> RowType
): Query<RowType> {
  return SimpleQuery(identifier, queries, driver, fileName, label, query, mapper)
}

private class SimpleQuery<out RowType : Any>(
  private val identifier: Int,
  queries: MutableList<Query<*>>,
  private val driver: SqlDriver,
  private val fileName: String,
  private val label: String,
  private val query: String,
  mapper: (SqlCursor) -> RowType
) : Query<RowType>(queries, mapper) {
  override fun <R> execute(block: (SqlCursor) -> R): R {
    return driver.executeQuery(identifier, query, 0, null, block)
  }

  override fun toString() = "$fileName:$label"
}

/**
 * A listenable, typed query generated by SQLDelight.
 *
 * @param RowType the type that this query can map its result set to.
 *
 * @property mapper The mapper this [Query] was created with, which can convert a row in the SQL
 *   cursor returned by [execute] to [RowType].
 */
abstract class Query<out RowType : Any>(
  private val queries: MutableList<Query<*>>,
  val mapper: (SqlCursor) -> RowType
) {
  private val listenerLock = QueryLock()
  private val listeners = sharedSet<Listener>()

  /**
   * Notify listeners that their current result set is staled.
   *
   * Called internally by SQLDelight when it detects a possible staling of the result set. Emits
   * some false positives but never misses a true positive.
   */
  fun notifyDataChanged() {
    listenerLock.withLock {
      listeners.forEach(Listener::queryResultsChanged)
    }
  }

  /**
   * Register a listener to be notified of future changes in the result set.
   */
  fun addListener(listener: Listener) {
    listenerLock.withLock {
      if (listeners.isEmpty()) queries.add(this)
      listeners.add(listener)
    }
  }

  /**
   * Remove a listener to no longer be notified of future changes in the result set.
   */
  fun removeListener(listener: Listener) {
    listenerLock.withLock {
      listeners.remove(listener)
      if (listeners.isEmpty()) queries.remove(this)
    }
  }

  /**
   * Execute the underlying statement. The resulting cursor is passed to the given block.
   *
   * The cursor is closed automatically after the block returns.
   */
  abstract fun <R> execute(block: (SqlCursor) -> R): R

  /**
   * @return The result set of the underlying SQL statement as a list of [RowType].
   */
  fun executeAsList(): List<RowType> = execute { cursor ->
    val result = mutableListOf<RowType>()
    while (cursor.next()) result.add(mapper(cursor))
    result
  }

  /**
   * @return The only row of the result set for the underlying SQL statement as a non null
   *   [RowType].
   *
   * @throws NullPointerException if when executed this query has no rows in its result set.
   * @throws IllegalStateException if when executed this query has multiple rows in its result set.
   */
  fun executeAsOne(): RowType {
    return executeAsOneOrNull()
      ?: throw NullPointerException("ResultSet returned null for $this")
  }

  /**
   * @return The first row of the result set for the underlying SQL statement as a non null
   *   [RowType] or null if the result set has no rows.
   *
   * @throws IllegalStateException if when executed this query has multiple rows in its result set.
   */
  fun executeAsOneOrNull(): RowType? = execute { cursor ->
    if (!cursor.next()) return@execute null
    val value = mapper(cursor)
    check(!cursor.next()) { "ResultSet returned more than 1 row for $this" }
    value
  }

  /**
   * An interface for listening to changes in the result set of a query.
   */
  interface Listener {
    /**
     * Called whenever the query this listener was attached to is dirtied.
     */
    fun queryResultsChanged()
  }
}
