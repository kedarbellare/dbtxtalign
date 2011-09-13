package cc.dbtxtalign

import params.SegmentParams
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer}

/**
 * @author kedar
 */


trait ASegmentAndMatch[Feature, Example <: AFeatAlignmentMentionExample[Feature]]
  extends ASegmentationMatchOnlyInferencer[Feature, Example] {
  def approxMatchers: Seq[PhrasePairScorer]

  def approxMatchThresholds: Seq[Double]

  override def scoreStart(a: Int, j: Int) = 0.0

  override def scoreTransition(a: Int, b: Int, i: Int, j: Int) = 0.0

  def scoreSingleEmission(a: Int, k: Int) = 0.0

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {}

  override def scoreEmission(a: Int, i: Int, j: Int) = {
    var bestMatchScore = 0.0
    otherSegmentation.segments.filter(_.label == a).foreach(oseg => {
      bestMatchScore = math.max(bestMatchScore, simScore(i, j, oseg.begin, oseg.end,
        approxMatchThresholds(a), approxMatchers(a)))
    })
    bestMatchScore
  }
}

class HMMSegmentAndMatch(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                         val approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                         val approxMatchThresholds: Seq[Double], val ex: FeatAlignmentMentionExample,
                         val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentAndMatch[Int, FeatAlignmentMentionExample]

class CRFSegmentAndMatch(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                         val approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                         val approxMatchThresholds: Seq[Double], val ex: FeatVecAlignmentMentionExample,
                         val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentAndMatch[FtrVec, FeatVecAlignmentMentionExample]