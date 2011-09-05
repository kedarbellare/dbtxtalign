package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec

/**
 * @author kedar
 */


trait AFeatAlignmentMentionExample[Feature] extends AFeatMentionExample[Feature] {
  var otherSegmentations: Seq[Segmentation] = null

  def otherIds: Seq[String]

  def otherWords: Seq[Seq[String]]
}

class FeatAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val featSeq: Seq[Int], val trueSegmentation: Segmentation,
                                  val otherIds: Seq[String], val otherWords: Seq[Seq[String]])
  extends AFeatAlignmentMentionExample[Int]

class FeatVecAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val featSeq: Seq[FtrVec], val trueSegmentation: Segmentation,
                                     val otherIds: Seq[String], val otherWords: Seq[Seq[String]])
  extends AFeatAlignmentMentionExample[FtrVec]