package cc.dbtxtalign.params

import collection.mutable.HashMap

/**
 * @author kedarb
 * @since 3/27/11
 */

class HashWeightVec[K](defaultValue: Double = 0.0) extends HashMap[K, Double] {
  override def default(key: K) = defaultValue

  def increment_!(key: K, value: Double): HashWeightVec[K] = {
    val newValue = apply(key) + value
    update(key, newValue)
    this
  }

  def increment_!(that: HashWeightVec[K], scale: Double): HashWeightVec[K] = {
    if (scale != 0.0) for ((key, value) <- that) increment_!(key, value * scale)
    this
  }

  def increment_!(f: (K) => Double): HashWeightVec[K] = {
    for ((key, value) <- this) increment_!(key, f(key))
    this
  }

  def div_!(scale: Double) = {
    for ((key, value) <- this) update(key, value / scale)
    this
  }

  def dot(that: HashWeightVec[K]): Double = innerJoin(that, _ * _)

  def intersect(that: HashWeightVec[K]): Double = innerJoin(that, math.min(_, _))

  def booleanIntersect(that: HashWeightVec[K]): Double =
    innerJoin(that, (a: Double, b: Double) => {
      if (a > 0 && b > 0) 1 else 0
    })

  private def innerJoin(that: HashWeightVec[K], oper: (Double, Double) => Double): Double = {
    var result = 0.0
    if (this.size > that.size) {
      for ((key, value) <- that; thisValue = this(key)) result += oper(value, thisValue)
    } else {
      for ((key, value) <- this; thatValue = that(key)) result += oper(value, thatValue)
    }
    result
  }

  def project_!(f: (K, Double) => Double) = {
    for ((key, value) <- this) update(key, f(key, value))
    this
  }

  def squaredNorm = {
    var result = 0.0
    for ((key, value) <- this) result += (value * value)
    result
  }

  def absNorm = {
    var result = 0.0
    for ((key, value) <- this) result += math.abs(value)
    result
  }

  def toUnitVector = {
    val len = norm2
    if (len > 0) div_!(1.0 / len) else this
  }

  def norm2 = math.sqrt(squaredNorm)

  def norm1 = absNorm

  def regularize_!(scale: Double) = {
    if (scale != 0.0) for ((key, value) <- this) increment_!(key, value * scale)
    this
  }
}

class DefaultHashWeightVec(val defaultValue: Double = 0.0) extends HashWeightVec[Any](defaultValue)

class CountVec(val defaultValue: Double = 0.0) extends HashWeightVec[String](defaultValue)