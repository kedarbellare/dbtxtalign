package cc.dbtxtalign.blocking

import collection.mutable.{HashMap, HashSet}
import cc.dbtxtalign.Mention

/**
 * @author kedar
 */


trait AbstractBlocker {
  def orderedIds(id1: String, id2: String): (String, String) = {
    if (id1.hashCode() < id2.hashCode()) id1 -> id2 else id2 -> id1
  }

  def isPair(id1: String, id2: String): Boolean

  def numPairs: Int

  def getAllPairs: HashSet[(String, String)]

  def getRecall(cluster2ids: HashMap[String, Seq[String]], id2mention: HashMap[String, Mention],
                printErrors: Boolean = false) = {
    var numPairs = 0
    var numFound = 0
    for (ids <- cluster2ids.values) {
      for (i <- 0 until ids.size; j <- i + 1 until ids.size) {
        if (isPair(ids(i), ids(j))) {
          numFound += 1
        } else if (printErrors) {
          println("\tmissed: [" + id2mention(ids(i)).isRecord + "][" + id2mention(ids(i)).words.mkString(" ") + "]\t[" +
            id2mention(ids(j)).isRecord + "][" + id2mention(ids(j)).words.mkString(" ") + "]")
        }
        numPairs += 1
      }
    }
    println("hash: " + numFound + "/" + numPairs)
    (1.0 * numFound) / numPairs
  }
}