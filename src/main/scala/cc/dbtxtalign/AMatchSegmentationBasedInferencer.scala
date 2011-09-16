package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.AParams
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */


trait AMatchSegmentationBasedInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature], Params <: AParams]
  extends ASegmentationBasedInferencer[Feature, MatchSegmentation, Example, Params] {
  def newWidget = new MatchSegmentation(false, new Segmentation(N))

  def trueMatchInfer: Boolean

  def trueSegmentInfer: Boolean

  def allowedMatch(isMatch: Boolean): Boolean = (!trueInfer && !trueMatchInfer) || isMatch == ex.trueWidget.isMatch

  override def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueSegmentInfer) {
      allowedSegmentTruth(a, i, j)
    } else {
      super.allowedSegment(a, i, j)
    }
  }
}