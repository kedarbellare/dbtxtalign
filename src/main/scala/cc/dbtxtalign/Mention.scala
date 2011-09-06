package cc.dbtxtalign

import collection.mutable.ArrayBuffer
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.utils.Utils

/**
 * @author kedar
 */

object SegmentationHelper {
  def extractSegmentsForLabel(words: Seq[String], bioLabels: Seq[String], label: String): Seq[Seq[String]] = {
    val buff = new ArrayBuffer[Seq[String]]
    val segbuff = new ArrayBuffer[String]
    for (i <- 0 until bioLabels.size) {
      val currLbl = bioLabels(i)
      if (currLbl == "O" || currLbl.startsWith("B-")) {
        if (segbuff.size > 0) {
          buff += segbuff.clone().toSeq
          segbuff.clear()
        }
      }
      if (currLbl.length() >= 2 && currLbl.substring(2) == label) {
        segbuff += words(i)
      }
    }
    if (segbuff.size > 0) buff += segbuff.toSeq
    buff.toSeq
  }

  def toFullString(words: Seq[String], segmentation: Segmentation, lstr: (Int) => String): String = {
    val buff = new StringBuffer
    buff.append(">>\n>words\n")
    Utils.foreachIndex(words.toArray, {
      (i: Int, w: String) => buff.append(i).append('\t')
        .append('"').append(w).append('"').append('\n')
    })
    buff.append("\n>labels\n")
    Utils.forIndex(segmentation.numSegments, {
      i: Int =>
        val segment = segmentation.segment(i)
        val lbl = lstr(segment.label)
        if (lbl != "O") buff.append(segment.begin).append('\t').append(segment.end - 1).append('\t')
          .append('"').append(lbl).append('"').append('\n')
    })
    buff.append('\n')
    buff.toString
  }
}

class Mention(val id: String, val isRecord: Boolean, val words: Seq[String], val trueBioLabels: Seq[String]) {
  def extractTrueSegmentsFor(label: String): Seq[Seq[String]] =
    SegmentationHelper.extractSegmentsForLabel(words, trueBioLabels, label)

  def extractTrueWordsFor(label: String): Seq[String] =
    extractTrueSegmentsFor(label).flatMap(identity(_))

  override def toString = (if (isRecord) "record[" else "text[") + id + "]\n" +
    words.mkString(" ") + "\n" +
    trueBioLabels.mkString(" ") + "\n"
}