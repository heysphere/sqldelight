package com.squareup.sqldelight.drivers.native

import co.touchlab.stately.collections.frozenHashSet
import co.touchlab.stately.concurrency.ThreadLocalRef
import co.touchlab.stately.concurrency.value
import com.squareup.sqldelight.drivers.native.util.Transferrable
import com.squareup.sqldelight.drivers.native.util.TransferrableBox
import kotlin.native.concurrent.ThreadLocal
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.microseconds

@ThreadLocal
var state: Bench.State = Bench.State()

class Bench {
  class State(var counter: MutableSet<Int> = mutableSetOf()): Transferrable<State> {
    override fun mutableDeepCopy(): State = State(counter.toMutableSet())
    override fun freeze(): State = freeze()
  }

  @Test
  fun thread_local() {
    measureAndReport("thread_local", 100_000) {
      state.counter.add(1)
    }
  }

  @Test
  fun stately_thread_local() {
    val box = ThreadLocalRef<State>()
    box.value = State()

    measureAndReport("stately_thread_local", 100_000) {
      box.value!!.counter.add(1)
    }
  }

  @Test
  fun stately_frozenSet() {
    val box = frozenHashSet<Int>()

    measureAndReport("stately_frozenSet", 100_000) {
      box.add(1)
    }
  }

  @Test
  fun attach_to_box_in_each_itr() {
    val box = TransferrableBox(State())
    measureAndReport("attach_to_box_in_each_itr", 100_000) {
      box.attachToCurrentWorker()
      box.withValue { it.counter.add(1) }
      box.detachFromCurrentWorker()
    }
  }

  @Test
  fun attach_to_box_once() {
    val box = TransferrableBox(State())
    box.attachToCurrentWorker()
    measureAndReport("attach_to_box_once",100_000) {
      box.withValue { it.counter.add(1) }
    }
    box.detachFromCurrentWorker()
  }

  @Test
  fun no_boxing() {
    val state = State()
    measureAndReport("no_boxing", 100_000) {
      state.counter.add(1)
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