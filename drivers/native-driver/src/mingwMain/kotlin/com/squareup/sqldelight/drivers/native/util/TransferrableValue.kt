package com.squareup.sqldelight.drivers.native.util

import kotlin.native.concurrent.AtomicReference
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.native.concurrent.WorkerBoundReference
import kotlin.native.concurrent.freeze
import kotlin.native.concurrent.isFrozen
import kotlin.native.concurrent.withWorker

@SymbolName("Kotlin_Any_share")
private external fun Any.share()

//actual open class TransferrableBox<T> actual constructor(initial: T) where T: Any, T: Transferrable<T> {
//  @PublishedApi
//  internal val value = initial.apply { share() }
//
//  actual inline fun <R> withValue(action: (T) -> R): R {
//    return action(value)
//  }
//
//  actual inline fun attachToCurrentWorker() {}
//  actual inline fun detachFromCurrentWorker() {}
//
//  actual open fun close() {}
//}

actual fun <T> createOnOtherWorker(action: () -> T): T = withWorker {
  execute(TransferMode.SAFE, { action.freeze() }, { it.invoke() }).result
}

actual open class TransferrableBox<T> actual constructor(initial: T) where T: Any, T: Transferrable<T> {
  val stateRef = AtomicReference<Any?>(initial.freeze())

  actual inline fun <R> withValue(action: (T) -> R): R {
    @Suppress("UNCHECKED_CAST")
    val current = stateRef.value as WorkerBoundReference<*>

    // [WorkerBoundReference.value] does check worker. So we don't need to validate it here.
    @Suppress("UNCHECKED_CAST")
    return action(current.value as T)
  }

  actual inline fun attachToCurrentWorker() {
    val current = stateRef.value

    @Suppress("UNCHECKED_CAST")
    val frozen = current as T

    stateRef.value = WorkerBoundReference(frozen.mutableDeepCopy()).freeze()
  }

  actual inline fun detachFromCurrentWorker() {
    @Suppress("UNCHECKED_CAST")
    val current = stateRef.value as WorkerBoundReference<T>
    stateRef.value = current.value.freeze()
  }

  actual open fun close() {
    stateRef.value = null
  }
}

@PublishedApi
internal const val BoxWasDisposedOf = """
The `TransferrableBox<T>` has been disposed of.
"""

@PublishedApi
internal const val IllegalConcurrentCall = """
Illegal concurrent call to `TransferrableBox<T>`. Access to the box must be serialized.
"""

@PublishedApi
internal const val IllegalAccessToUnattachedBox = """
Illegal access to an unattached `TransferrableBox<T>`.
"""