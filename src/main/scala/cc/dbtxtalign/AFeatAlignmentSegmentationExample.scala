package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import collection.mutable.{HashSet, HashMap}

/**
 * @author kedar
 */

class MatchSegmentation(val matchIds: HashSet[String], val segmentation: Segmentation) {
  override def toString = segmentation.toString() + " matches[" + matchIds.mkString(", ") + "]"
}

trait AFeatAlignmentSegmentationExample[Feature, Widget] extends AFeatSegmentationExample[Feature, Widget] {
  def otherIds: Seq[String]

  def otherWordsSeq: Seq[Seq[String]]

  def otherSegmentations: Seq[Segmentation]
}

trait AFeatAlignmentMentionExample[Feature] extends AFeatAlignmentSegmentationExample[Feature, MatchSegmentation]

class FeatAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                  val possibleEnds: Seq[Boolean], val featSeq: Seq[Int],
                                  val trueMatchIds: HashSet[String], val trueSegmentation: Segmentation,
                                  val otherIds: Seq[String], val otherWordsSeq: Seq[Seq[String]],
                                  val otherSegmentations: Seq[Segmentation])
  extends AFeatAlignmentMentionExample[Int] {
  def this(id: String, isRecord: Boolean, words: Seq[String],
           possibleEnds: Seq[Boolean], featSeq: Seq[Int],
           trueMatchIds: HashSet[String], trueSegmentation: Segmentation,
           otherId: String, otherWords: Seq[String],
           otherSegmentation: Segmentation) = {
    this (id, isRecord, words, possibleEnds, featSeq, trueMatchIds, trueSegmentation,
      Seq(otherId), Seq(otherWords), Seq(otherSegmentation))
  }

  val trueWidget = new MatchSegmentation(trueMatchIds, trueSegmentation)
}

class FeatVecAlignmentMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                                     val possibleEnds: Seq[Boolean], val featSeq: Seq[FtrVec],
                                     val trueMatchIds: HashSet[String], val trueSegmentation: Segmentation,
                                     val otherIds: Seq[String], val otherWordsSeq: Seq[Seq[String]],
                                     val otherSegmentations: Seq[Segmentation])
  extends AFeatAlignmentMentionExample[FtrVec] {
  def this(id: String, isRecord: Boolean, words: Seq[String],
           possibleEnds: Seq[Boolean], featSeq: Seq[FtrVec],
           trueMatchIds: HashSet[String], trueSegmentation: Segmentation,
           otherId: String, otherWords: Seq[String],
           otherSegmentation: Segmentation) = {
    this (id, isRecord, words, possibleEnds, featSeq, trueMatchIds, trueSegmentation,
      Seq(otherId), Seq(otherWords), Seq(otherSegmentation))
  }

  val trueWidget = new MatchSegmentation(trueMatchIds, trueSegmentation)
}
