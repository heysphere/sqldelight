package com.squareup.sqldelight.drivers.native.util

import kotlin.native.concurrent.AtomicReference
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.native.concurrent.WorkerBoundReference
import kotlin.native.concurrent.freeze
import kotlin.native.concurrent.isFrozen

actual open class TransferrableBox<T> actual constructor(initial: T) where T: Any, T: Transferrable<T> {
  val stateRef = AtomicReference<WorkerBoundReference<T>?>(
    WorkerBoundReference(initial).freeze()
  )

  actual fun <R> withValue(action: (T) -> R): R {
    val current = stateRef.value
    check(current is WorkerBoundReference<*>) { IllegalAccessToUnattachedBox }

    // [WorkerBoundReference.value] does check worker. So we don't need to validate it here.
    @Suppress("UNCHECKED_CAST")
    return action(current.value as T)
  }

  actual fun attachToCurrentWorker() {
    when (val current = stateRef.value) {
      is WorkerBoundReference<*> -> {
        if (current.worker != Worker.current) {
          // Transfer just-in-time.
          val transferredValue = current.worker
            .execute(TransferMode.SAFE, { current }, { it.value })
            .result

          val new = WorkerBoundReference(transferredValue).freeze()
          val done = stateRef.compareAndSet(current, new)
          check(done) { IllegalConcurrentCall }
        }
      }
      null -> throw IllegalStateException(BoxWasDisposedOf)
      else -> {
        check(current.isFrozen)

        @Suppress("UNCHECKED_CAST")
        val frozen = current as T

        val new = WorkerBoundReference(frozen.mutableDeepCopy()).freeze()
        val done = stateRef.compareAndSet(current, new)

        check(done) { IllegalConcurrentCall }
      }
    }
  }

  actual inline fun detachFromCurrentWorker() {}

  actual open fun close() {
    stateRef.value = null
  }
}

internal const val BoxWasDisposedOf = """
The `TransferrableBox<T>` has been disposed of.
"""

internal const val IllegalConcurrentCall = """
Illegal concurrent call to `TransferrableBox<T>`. Access to the box must be serialized.
"""

internal const val IllegalAccessToUnattachedBox = """
Illegal access to an unattached `TransferrableBox<T>`.
"""