package cc.dbtxtalign

import optimization.gradientBasedMethods.Objective
import cc.refectorie.user.kedarb.dynprog.la.ArrayFromVectors
import cc.refectorie.user.kedarb.dynprog.{ProbStats, AParams}
import org.apache.log4j.Logger

/**
 * @author kedar
 */

abstract class ACRFObjective[Params <: AParams](params: Params, invVariance: Double) extends Objective {
  val paramsArrayFromVectors = new ArrayFromVectors(params.getWtVecs)
  var objectiveValue = Double.NaN

  // set parameters and gradient
  parameters = new Array[Double](paramsArrayFromVectors.vectorsArraySize)
  paramsArrayFromVectors.getVectorsInArray(parameters)
  gradient = new Array[Double](paramsArrayFromVectors.vectorsArraySize)

  def logger: Logger

  override def getParameter(index: Int) = parameters(index)

  override def setParameter(index: Int, value: Double) {
    updateCalls += 1
    objectiveValue = Double.NaN
    parameters(index) = value
  }

  override def getParameters = parameters

  override def setParameters(params: Array[Double]) {
    updateCalls += 1
    objectiveValue = Double.NaN
    require(params.length == getNumParameters)
    Array.copy(params, 0, parameters, 0, params.length)
  }

  override def setInitialParameters(params: Array[Double]) {
    setParameters(params)
  }

  def getValueAndGradient: (Params, ProbStats)

  def updateValueAndGradient() {
    // set parameters as they may have changed
    paramsArrayFromVectors.setVectorsFromArray(parameters)
    val (expectations, stats) = getValueAndGradient
    // output objective
    objectiveValue = 0.5 * params.wtTwoNormSquared * invVariance - stats.logZ
    logger.info("objective=" + objectiveValue)
    // compute gradient
    java.util.Arrays.fill(gradient, 0.0)
    expectations.wtAdd_!(params, -invVariance)
    // point in correct direction
    expectations.wtDiv_!(-1)
    // move expectations to gradient
    new ArrayFromVectors(expectations.getWtVecs).getVectorsInArray(gradient)
  }

  def getValue = {
    if (objectiveValue.isNaN) {
      functionCalls += 1
      updateValueAndGradient()
    }
    objectiveValue
  }

  def getGradient = {
    if (objectiveValue.isNaN) {
      gradientCalls += 1
      updateValueAndGradient()
    }
    gradient
  }

  override def toString = "objective = " + objectiveValue
}