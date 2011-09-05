package cc.dbtxtalign

import io.Source
import collection.mutable.{HashMap, ArrayBuffer}

/**
 * @author kedar
 */

object FileHelper {
  def getRawMentions(isRecord: Boolean, filename: String, simplify: (String) => String = identity(_)): Seq[Mention] = {
    val buff = new ArrayBuffer[Mention]
    val lineIter = Source.fromFile(filename).getLines()
    while (lineIter.hasNext) {
      val id = lineIter.next()
      val words = lineIter.next().split("\t").map(simplify(_))
      val bioLabels = lineIter.next().split("\t")
      // empty line
      lineIter.next()
      buff += new Mention(id, isRecord, words, bioLabels)
    }
    buff.toSeq
  }

  def getMentionClusters(filename: String): HashMap[String, String] = {
    val id2cluster = new HashMap[String, String]
    Source.fromFile(filename).getLines().foreach(line => {
      val parts = line.split("\t")
      id2cluster(parts(0)) = parts(1)
    })
    id2cluster
  }
}

