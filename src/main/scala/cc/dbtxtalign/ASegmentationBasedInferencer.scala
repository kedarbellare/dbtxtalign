package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.{AHypergraphInferState, AParams}
import cc.refectorie.user.kedarb.dynprog.segment.{Segment, Segmentation}
import cc.refectorie.user.kedarb.dynprog.types.{ParamVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._

/**
 * @author kedar
 */


trait ASegmentationBasedInferencer[Feature, Widget, Example <: AFeatSegmentationExample[Feature, Widget], Params <: AParams]
  extends AHypergraphInferState[Widget, Example, Params] {
  def labelIndexer: Indexer[String]

  def maxLengths: Seq[Int]

  lazy val L: Int = labelIndexer.size

  lazy val otherLabelIndex: Int = labelIndexer.indexOf_!("O")

  lazy val N: Int = ex.numTokens

  lazy val isRecord: Boolean = ex.isRecord

  lazy val words: Seq[String] = ex.words

  lazy val trueSegmentation: Segmentation = ex.trueSegmentation

  lazy val trueLabelDefined = mapIndex(N, (i: Int) => trueSegmentation.labelAt(i).isDefined)

  lazy val cachedEmissionScores: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => Double.NaN))

  lazy val cachedEmissionCounts: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => 0.0))

  lazy val cachedPossibleEnds: Array[Boolean] = mapIndex(N + 1, (j: Int) => ex.isPossibleEnd(j))

  def allowedSegmentTruth(a: Int, i: Int, j: Int): Boolean = {
    if (a == otherLabelIndex) {
      forIndex(i, j, (k: Int) => {
        if (trueLabelDefined(k) && trueSegmentation.labelAt(k).get != a) return false
      })
      true
    }
    else if (trueSegmentation.contains(Segment(i, j, a))) true
    else {
      forIndex(i, j, (k: Int) => {
        if (trueLabelDefined(k)) return false
      })
      true
    }
  }

  def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueInfer) {
      allowedSegmentTruth(a, i, j)
    } else if (isRecord) {
      // only allow sub-segments that are a subset of segment at i
      val optionSegmentAtI = trueSegmentation.segmentAt(i)
      optionSegmentAtI.isDefined && optionSegmentAtI.get.end >= j &&
        (a == otherLabelIndex || cachedPossibleEnds(j) || optionSegmentAtI.get.end == j)
    } else {
      // allow all possible segments
      a == otherLabelIndex || cachedPossibleEnds(j)
    }
  }

  def allowedStart(a: Int): Boolean = true

  def allowedTransition(a: Int, b: Int): Boolean = true

  def transitionParams: Array[ParamVec] = throw fail("Not implemented!")

  def transitionCounts: Array[ParamVec] = throw fail("Not implemented!")

  def startParams: ParamVec = throw fail("Not implemented!")

  def startCounts: ParamVec = throw fail("Not implemented!")

  def emissionParams: Array[ParamVec] = throw fail("Not implemented!")

  def emissionCounts: Array[ParamVec] = throw fail("Not implemented!")

  def alignParams: Array[ParamVec] = throw fail("Not implemented!")

  def alignCounts: Array[ParamVec] = throw fail("Not implemented!")

  def scoreTransition(a: Int, b: Int, i: Int, j: Int): Double = {
    if (isRecord) 0.0
    else score(transitionParams(a), b)
  }

  def scoreStart(a: Int, j: Int): Double = {
    if (isRecord) 0.0
    else score(startParams, a)
  }

  def scoreSingleEmission(a: Int, k: Int): Double

  def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double) {
    if (!isRecord) update(transitionCounts(a), b, x)
  }

  def updateStart(a: Int, j: Int, x: Double) {
    if (!isRecord) update(startCounts, a, x)
  }

  def updateSingleEmission(a: Int, k: Int, x: Double) {
    cachedEmissionCounts(a)(k) += x
  }

  def scoreEmission(a: Int, i: Int, j: Int): Double = {
    var sumScore = 0.0
    forIndex(i, j, (k: Int) => {
      if (cachedEmissionScores(a)(k).isNaN) cachedEmissionScores(a)(k) = scoreSingleEmission(a, k)
      sumScore += cachedEmissionScores(a)(k)
    })
    sumScore
  }

  def updateEmission(a: Int, i: Int, j: Int, x: Double) {
    forIndex(i, j, (k: Int) => updateSingleEmission(a, k, x))
  }

  def updateSingleEmissionCached(a: Int, k: Int, x: Double)

  def genStartSegments(addStartEdge: Segment => Any) {
    forIndex(L, (a: Int) => {
      forIndex(1, math.min(maxLengths(a), N) + 1, (i: Int) => {
        if (allowedStart(a) && allowedSegment(a, 0, i)) {
          addStartEdge(Segment(0, i, a))
        }
      })
    })
  }

  def genTransitionSegments(a: Int, i: Int, addTransitionEdge: Segment => Any) {
    forIndex(L, (b: Int) => {
      forIndex(i + 1, math.min(i + maxLengths(b), N) + 1, (j: Int) => {
        if (allowedTransition(a, b) && allowedSegment(b, i, j)) {
          addTransitionEdge(Segment(i, j, b))
        }
      })
    })
  }

  override def updateCounts {
    super.updateCounts
    counts.synchronized {
      forIndex(L, (a: Int) => {
        forIndex(N, (k: Int) => {
          updateSingleEmissionCached(a, k, cachedEmissionCounts(a)(k))
        })
      })
    }
  }
}