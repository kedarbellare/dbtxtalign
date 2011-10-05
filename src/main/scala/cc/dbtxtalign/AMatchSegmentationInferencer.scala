package cc.dbtxtalign

import params.Params
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.InferSpec

/**
 * @author kedar
 */


trait AMatchSegmentationInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature]]
  extends AMatchSegmentationBasedInferencer[Feature, Example, Params] {
  override lazy val transitionParams = params.transitions.transitions

  override lazy val transitionCounts = counts.transitions.transitions

  override lazy val startParams = params.transitions.starts

  override lazy val startCounts = counts.transitions.starts

  override lazy val emissionParams = params.emissions.emissions

  override lazy val emissionCounts = counts.emissions.emissions

  override lazy val alignParams = params.aligns.labelAligns

  override lazy val alignCounts = counts.aligns.labelAligns

  // (label, phrase, otherPhrase) => alignFeatureVector
  def alignFeaturizer: (Int, Seq[String], Seq[String]) => FtrVec
}

class CRFMatchSegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                                     val ex: FeatVecAlignmentMentionExample,
                                     val params: Params, val counts: Params, val ispec: InferSpec,
                                     val trueMatchInfer: Boolean, val trueSegmentInfer: Boolean,
                                     val alignFeaturizer: (Int, Seq[String], Seq[String]) => FtrVec)
  extends AMatchSegmentationInferencer[FtrVec, FeatVecAlignmentMentionExample] {
  lazy val featSeq: Seq[FtrVec] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    if (!x.isNaN) update(emissionCounts(a), featSeq(k), x)
  }

  def scoreSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int) = {
    //    val key = SegmentAlignment(otherIds(otherIndex), a, i, j, oi, oj)
    //    if (!ex.cachedAlignFeatures.contains(key))
    //      ex.cachedAlignFeatures(key) = alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj))
    //    score(alignParams(a), ex.cachedAlignFeatures(key))
    score(alignParams(a), alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj)))
  }

  def updateSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) {
    //    val key = SegmentAlignment(otherIds(otherIndex), a, i, j, oi, oj)
    //    if (!ex.cachedAlignFeatures.contains(key))
    //      ex.cachedAlignFeatures(key) = alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj))
    //    update(alignCounts(a), ex.cachedAlignFeatures(key), v)
    update(alignCounts(a), alignFeaturizer(a, words.slice(i, j), otherWordsSeq(otherIndex).slice(oi, oj)), v)
  }
}
