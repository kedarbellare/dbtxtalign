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
  def alignFeaturizer: (Int, String, Int, Int, String, Int, Int) => FtrVec
}

class CRFMatchSegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int],
                                     val alignFeaturizer: (Int, String, Int, Int, String, Int, Int) => FtrVec,
                                     val ex: FeatVecAlignmentMentionExample,
                                     val params: Params, val counts: Params, val ispec: InferSpec,
                                     val trueMatchInfer: Boolean, val trueSegmentInfer: Boolean)
  extends AMatchSegmentationInferencer[FtrVec, FeatVecAlignmentMentionExample] {
  lazy val featSeq: Seq[FtrVec] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    if (!x.isNaN) update(emissionCounts(a), featSeq(k), x)
  }

  def scoreSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int) = {
    score(alignParams(a), alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj))
  }

  def updateSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) {
    update(alignCounts(a), alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj), v)
  }
}
