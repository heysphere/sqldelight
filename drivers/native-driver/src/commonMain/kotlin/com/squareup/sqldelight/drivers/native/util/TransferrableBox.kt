package com.squareup.sqldelight.drivers.native.util

@Suppress("NO_ACTUAL_FOR_EXPECT")
expect open class TransferrableBox<T>(initial: T) where T: Any, T: Transferrable<T> {
  fun <R> withValue(action: (T) -> R): R
  fun attachToCurrentWorker()
  fun detachFromCurrentWorker()
  open fun close()
}