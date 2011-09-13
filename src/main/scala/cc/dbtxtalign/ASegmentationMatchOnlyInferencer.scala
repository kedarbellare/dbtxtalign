package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import phrasematch._

/**
 * @author kedar
 */


trait ASegmentationMatchOnlyInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature]]
  extends ASegmentationBasedInferencer[Feature, Example] {
  type PhrasePairScorer = (Seq[String], Seq[String]) => Double

  lazy val otherWords: Seq[String] = ex.otherWords

  lazy val otherSegmentation: Segmentation = ex.otherSegmentation

  def simScore(i: Int, j: Int, oi: Int, oj: Int, threshold: Double,
               scorer: PhrasePairScorer = JaccardScorer.score(_, _)): Double = {
    val score = scorer(words.slice(i, j), otherWords.slice(oi, oj))
    if (score >= threshold) (score - threshold) / (1.0 - threshold)
    else (score - threshold) / threshold
  }
}