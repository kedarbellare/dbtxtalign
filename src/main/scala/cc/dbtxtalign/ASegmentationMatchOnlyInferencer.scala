package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */


trait ASegmentationMatchOnlyInferencer[Feature, Example <: AFeatMatchOnlyMentionExample[Feature]]
  extends ASegmentationInferencer[Feature, Example] {
  lazy val otherId: String = ex.otherId

  lazy val otherWords: Seq[String] = ex.otherWords

  lazy val otherSegmentation: Segmentation = ex.otherSegmentation

  def scoreSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String]): Double

  def updateSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String], v: Double)

  def scoreSimilarity(a: Int, i: Int, j: Int, oi: Int, oj: Int): Double =
    scoreSimilarity(a, words.slice(i, j), otherWords.slice(oi, oj))

  def updateSimilarity(a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) {
    updateSimilarity(a, words.slice(i, j), otherWords.slice(oi, oj), v)
  }
}