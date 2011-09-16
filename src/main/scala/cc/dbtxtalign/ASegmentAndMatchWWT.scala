package cc.dbtxtalign

import params.SegmentParams
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer}

/**
 * @author kedar
 */


trait ASegmentAndMatchWWT[Feature, Example <: AFeatMatchOnlyMentionExample[Feature]]
  extends ASegmentationMatchOnlyInferencer[Feature, Example] {
  type PhrasePairScorer = (Seq[String], Seq[String]) => Double

  def approxMatchers: Seq[PhrasePairScorer]

  def approxMatchThresholds: Seq[Double]

  override def scoreStart(a: Int, j: Int) = 0.0

  override def scoreTransition(a: Int, b: Int, i: Int, j: Int) = 0.0

  def scoreSingleEmission(a: Int, k: Int) = 0.0

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {}

  def scoreSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String]) = {
    val score = approxMatchers(a)(phrase, otherPhrase)
    if (score >= approxMatchThresholds(a)) score
    else 0.0
  }

  def updateSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String], v: Double) {}

  // assumes all scores between 0 and 1
  override def scoreEmission(a: Int, i: Int, j: Int) = {
    if (a == otherLabelIndex) 0.0
    else {
      var bestMatchScore = Double.NegativeInfinity
      otherSegmentation.segments.filter(_.label == a).foreach(oseg => {
        val simScore = scoreSimilarity (a, i, j, oseg.begin, oseg.end)
        if (simScore > 0 && simScore > bestMatchScore) {
          bestMatchScore = simScore
        }
      })
      bestMatchScore
    }
  }
}

class HMMSegmentAndMatchWWT(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                         val approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                         val approxMatchThresholds: Seq[Double], val ex: FeatMatchOnlyMentionExample,
                         val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentAndMatchWWT[Int, FeatMatchOnlyMentionExample]

class CRFSegmentAndMatchWWT(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                         val approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                         val approxMatchThresholds: Seq[Double], val ex: FeatVecMatchOnlyMentionExample,
                         val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentAndMatchWWT[FtrVec, FeatVecMatchOnlyMentionExample]