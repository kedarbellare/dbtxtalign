package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import collection.mutable.HashMap

/**
 * @author kedar
 */

class MatchSegmentation(var isMatch: Boolean, var segmentation: Segmentation)

trait AFeatAlignmentSegmentationExample[Feature, Widget] extends AFeatSegmentationExample[Feature, Widget] {
  def otherId: String

  def otherWords: Seq[String]

  def otherSegmentation: Segmentation
}

trait AFeatMatchOnlyMentionExample[Feature]
  extends AFeatMentionExample[Feature] with AFeatAlignmentSegmentationExample[Feature, Segmentation]

trait AFeatAlignmentMentionExample[Feature] extends AFeatAlignmentSegmentationExample[Feature, MatchSegmentation] {
  def degree: Int
}

class FeatMatchOnlyMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val possibleEnds: Seq[Boolean], val featSeq: Seq[Int],
                                  val trueSegmentation: Segmentation,
                                  val otherId: String, val otherWords: Seq[String],
                                  val otherSegmentation: Segmentation)
  extends AFeatMatchOnlyMentionExample[Int]

class FeatVecMatchOnlyMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val possibleEnds: Seq[Boolean], val featSeq: Seq[FtrVec],
                                     val trueSegmentation: Segmentation,
                                     val otherId: String, val otherWords: Seq[String],
                                     val otherSegmentation: Segmentation)
  extends AFeatMatchOnlyMentionExample[FtrVec]

class FeatAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val possibleEnds: Seq[Boolean], val featSeq: Seq[Int],
                                  val trueMatch: Boolean, val trueSegmentation: Segmentation, val degree: Int,
                                  val otherId: String, val otherWords: Seq[String],
                                  val otherSegmentation: Segmentation) extends AFeatAlignmentMentionExample[Int] {
  val trueWidget = new MatchSegmentation(trueMatch, trueSegmentation)
}

class FeatVecAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val possibleEnds: Seq[Boolean], val featSeq: Seq[FtrVec],
                                     val trueMatch: Boolean, val trueSegmentation: Segmentation, val degree: Int,
                                     val otherId: String, val otherWords: Seq[String],
                                     val otherSegmentation: Segmentation) extends AFeatAlignmentMentionExample[FtrVec] {
  val trueWidget = new MatchSegmentation(trueMatch, trueSegmentation)

  val cachedAlignFeatures = new HashMap[(Int, Int, Int, Int, Int), FtrVec]
}