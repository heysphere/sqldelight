package com.squareup.sqldelight.drivers.native

import co.touchlab.stately.collections.frozenHashSet
import co.touchlab.stately.concurrency.ThreadLocalRef
import co.touchlab.stately.concurrency.value
import co.touchlab.stately.freeze
import com.squareup.sqldelight.drivers.native.util.Transferrable
import com.squareup.sqldelight.drivers.native.util.TransferrableBox
import com.squareup.sqldelight.drivers.native.util.createOnOtherWorker
import kotlin.native.concurrent.ThreadLocal
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.microseconds

@ThreadLocal
var state: Bench.State = Bench.State()

val range = 64..80

class Bench {
  class State(var counter: MutableSet<Int> = mutableSetOf<Int>().apply { addAll(1..128) }): Transferrable<State> {
    override fun mutableDeepCopy(): State = State(counter.toMutableSet())
  }

  @Test
  fun thread_local() {
    measureAndReport("thread_local", 100_000) {
      state.counter.addAll(range); state.counter.contains(range.start)
    }
  }

  @Test
  fun stately_thread_local() {
    val box = ThreadLocalRef<State>()
    box.value = State()

    measureAndReport("stately_thread_local", 100_000) {
      box.value!!.counter.addAll(range)
      box.value!!.counter.contains(range.start)
    }
  }

  @Test
  fun stately_frozenSet() {
    val box = frozenHashSet<Int>()

    measureAndReport("stately_frozenSet", 100_000) {
      box.addAll(range)
      box.contains(range.start)
    }
  }

  @Test
  fun shareable() {
    val box = createOnOtherWorker { TransferrableBox(State()).freeze() }

    measureAndReport("shareable", 100_000) {
      box.attachToCurrentWorker()
      box.withValue { it.counter.addAll(range); it.counter.contains(range.start) }
      box.detachFromCurrentWorker()
    }
  }

  @Test
  fun attach_to_box_in_each_itr() {
    val box = TransferrableBox(State())
    measureAndReport("attach_to_box_in_each_itr", 100_000) {
      box.attachToCurrentWorker()
      box.withValue { it.counter.addAll(range); it.counter.contains(range.start) }
      box.detachFromCurrentWorker()
    }
  }

  @Test
  fun attach_to_box_in_each_itr_2() {
    val box = TransferrableBox(State())
    measureAndReport("attach_to_box_in_each_itr_2", 100_000) {
      for (value in range) {
        box.attachToCurrentWorker()
        box.withValue { it.counter.add(value) }
        box.detachFromCurrentWorker()
      }
    }
  }

  @Test
  fun attach_to_box_once() {
    val box = TransferrableBox(State())
    box.attachToCurrentWorker()
    measureAndReport("attach_to_box_once",100_000) {
      box.withValue { it.counter.addAll(range); it.counter.contains(range.start) }
    }
    box.detachFromCurrentWorker()
  }

  @Test
  fun no_boxing() {
    val state = State()
    measureAndReport("no_boxing", 100_000) {
      state.counter.addAll(range); state.counter.contains(range.start)
    }
  }

  @OptIn(ExperimentalTime::class)
  private inline fun measureAndReport(name: String, iterations: Int, action: () -> Unit) {
    var min: Duration = (Int.MAX_VALUE).microseconds
    var avg: Duration = 0.microseconds
    var max: Duration = 0.microseconds

    repeat(iterations) {
      val duration = measureTime {
        action()
      }

      min = minOf(min, duration)
      max = maxOf(max, duration)
      avg = avg + duration / iterations
    }

    println("[$name] min: ${min.inMicroseconds} us; max: ${max.inMicroseconds} us; avg: ${avg.inMicroseconds} us")
  }
}