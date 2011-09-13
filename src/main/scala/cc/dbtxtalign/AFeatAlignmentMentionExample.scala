package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec

/**
 * @author kedar
 */


trait AFeatAlignmentMentionExample[Feature] extends AFeatMentionExample[Feature] {
  def otherWords: Seq[String]

  def otherSegmentation: Segmentation
}

class FeatAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val possibleEnds: Seq[Boolean], val featSeq: Seq[Int],
                                  val trueSegmentation: Segmentation,  val otherWords: Seq[String],
                                  val otherSegmentation: Segmentation)
  extends AFeatAlignmentMentionExample[Int]

class FeatVecAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val possibleEnds: Seq[Boolean], val featSeq: Seq[FtrVec],
                                     val trueSegmentation: Segmentation, val otherWords: Seq[String],
                                     val otherSegmentation: Segmentation)
  extends AFeatAlignmentMentionExample[FtrVec]