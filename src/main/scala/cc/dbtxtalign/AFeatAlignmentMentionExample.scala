package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec

/**
 * @author kedar
 */


trait AFeatAlignmentMentionExample[Feature] extends AFeatMentionExample[Feature] {
  def otherIds: Seq[String]
}

class FeatAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val possibleEnds: Seq[Boolean], val featSeq: Seq[Int],
                                  val trueSegmentation: Segmentation, val otherIds: Seq[String])
  extends AFeatAlignmentMentionExample[Int]

class FeatVecAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val possibleEnds: Seq[Boolean], val featSeq: Seq[FtrVec],
                                     val trueSegmentation: Segmentation, val otherIds: Seq[String])
  extends AFeatAlignmentMentionExample[FtrVec]