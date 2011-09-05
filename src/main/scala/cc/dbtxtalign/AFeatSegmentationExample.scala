package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.AExample
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */

trait AFeatSegmentationExample[Feature, Widget] extends AExample[Widget] {
  def id: String

  def isRecord: Boolean

  def words: Seq[String]

  def numTokens: Int = words.length

  def featSeq: Seq[Feature]

  def trueSegmentation: Segmentation

  def isPossibleEnd(j: Int): Boolean = true

  var predSegmentation: Segmentation = null
}

trait AFeatMentionExample[Feature] extends AFeatSegmentationExample[Feature, Segmentation] {
  def trueWidget = trueSegmentation
}

class FeatMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                         val featSeq: Seq[Int], val trueSegmentation: Segmentation)
  extends AFeatMentionExample[Int]

class FeatVecMentionExample(val id: String, val isRecord: Boolean, val words: Seq[String],
                            val featSeq: Seq[FtrVec], val trueSegmentation: Segmentation)
  extends AFeatMentionExample[FtrVec]