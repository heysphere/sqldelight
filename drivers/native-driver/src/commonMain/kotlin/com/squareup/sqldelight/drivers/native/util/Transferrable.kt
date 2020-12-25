package com.squareup.sqldelight.drivers.native.util

interface Transferrable<T> {
  /**
   * Produce a deep mutable copy of the receiver.
   */
  fun mutableDeepCopy(): T

  /**
   * Freeze and return the receiver.
   */
  fun freeze(): T
}