package cc.dbtxtalign

import params.AlignParams
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.InferSpec

/**
 * @author kedar
 */

trait AMatchOnlySegmentationInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature]]
  extends AMatchSegmentationBasedInferencer[Feature, Example, AlignParams] {
  override lazy val alignParams = params.labelAligns

  override lazy val alignCounts = counts.labelAligns

  override def scoreStart(a: Int, j: Int) = 0.0

  override def updateStart(a: Int, j: Int, x: Double) {}

  override def scoreTransition(a: Int, b: Int, i: Int, j: Int) = 0.0

  override def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double) {}

  override def scoreEmission(a: Int, i: Int, j: Int) = 0.0

  override def updateEmission(a: Int, i: Int, j: Int, x: Double) {}

  def scoreSingleEmission(a: Int, k: Int) = 0.0

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {}

  // (label, phrase, otherPhrase) => alignFeatureVector
  def alignFeaturizer: (Int, String, Int, Int, String, Int, Int) => FtrVec

  def scoreSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int) = {
    //    val key = SegmentAlignment(otherIds(otherIndex), a, i, j, oi, oj)
    //    if (!ex.cachedAlignFeatures.contains(key))
    //      ex.cachedAlignFeatures(key) = alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj))
    //    score(alignParams(a), ex.cachedAlignFeatures(key))
    score(alignParams(a), alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj))
  }

  def updateSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) {
    //    val key = SegmentAlignment(otherIds(otherIndex), a, i, j, oi, oj)
    //    if (!ex.cachedAlignFeatures.contains(key))
    //      ex.cachedAlignFeatures(key) = alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj))
    //    update(alignCounts(a), ex.cachedAlignFeatures(key), v)
    update(alignCounts(a), alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj), v)
  }
}

class HMMMatchOnlySegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                                         val alignFeaturizer: (Int, String, Int, Int, String, Int, Int) => FtrVec,
                                         val ex: FeatAlignmentMentionExample, val params: AlignParams,
                                         val counts: AlignParams, val ispec: InferSpec,
                                         val trueMatchInfer: Boolean, val trueSegmentInfer: Boolean)
  extends AMatchOnlySegmentationInferencer[Int, FeatAlignmentMentionExample]

class CRFMatchOnlySegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                                         val alignFeaturizer: (Int, String, Int, Int, String, Int, Int) => FtrVec,
                                         val ex: FeatVecAlignmentMentionExample, val params: AlignParams,
                                         val counts: AlignParams, val ispec: InferSpec,
                                         val trueMatchInfer: Boolean, val trueSegmentInfer: Boolean)
  extends AMatchOnlySegmentationInferencer[FtrVec, FeatVecAlignmentMentionExample]