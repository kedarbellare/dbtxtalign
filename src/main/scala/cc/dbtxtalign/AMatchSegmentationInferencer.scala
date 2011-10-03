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

  def alignFeatureIndexer: Indexer[String]
}

class CRFMatchSegmentationInferencer(val labelIndexer: Indexer[String], val alignFeatureIndexer: Indexer[String],
                                     val maxLengths: Seq[Int], val ex: FeatVecAlignmentMentionExample,
                                     val params: Params, val counts: Params, val ispec: InferSpec,
                                     val trueMatchInfer: Boolean, val trueSegmentInfer: Boolean,
                                     val alignFeatureVector: (Int, Seq[String], Seq[String]) => FtrVec)
  extends AMatchSegmentationInferencer[FtrVec, FeatVecAlignmentMentionExample] {
  lazy val featSeq: Seq[FtrVec] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    if (!x.isNaN) update(emissionCounts(a), featSeq(k), x)
  }

  def scoreSimilarity(a: Int, i: Int, j: Int, oi: Int, oj: Int) = {
    val key = (a, i, j, oi, oj)
    if (!ex.cachedAlignFeatures.contains(key))
      ex.cachedAlignFeatures(key) = alignFeatureVector(a, words.slice(i, j), otherWords.slice(oi, oj))
    score(alignParams(a), ex.cachedAlignFeatures(key))
  }

  def updateSimilarity(a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) = {
    val key = (a, i, j, oi, oj)
    if (!ex.cachedAlignFeatures.contains(key))
      ex.cachedAlignFeatures(key) = alignFeatureVector(a, words.slice(i, j), otherWords.slice(oi, oj))
    update(alignCounts(a), ex.cachedAlignFeatures(key), v)
  }
}

