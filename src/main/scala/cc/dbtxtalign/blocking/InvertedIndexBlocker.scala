package cc.dbtxtalign.blocking

import cc.dbtxtalign.Mention
import collection.mutable.{ArrayBuffer, HashMap, HashSet}

/**
 * @author kedar
 */

class InvertedIndexBlocker(val maxBlockSize: Int, val mentions: Seq[Mention],
                           val recordExtractor: Mention => HashSet[String],
                           val textExtractor: Mention => HashSet[String]) extends AbstractBlocker {
  private val validKeys = initValidKeys()
  private val invertedIndex = initInvertedIndex()
  private val allPairs = initPairs()

  private def initValidKeys(): HashSet[String] = {
    val keys = new HashSet[String]
    // first collect record keys
    for (m <- mentions if m.isRecord) keys ++= recordExtractor(m)
    // count vector
    val counts = new HashMap[String, Int]
    for (m <- mentions) {
      val mentionKeys = if (m.isRecord) recordExtractor(m) else textExtractor(m)
      mentionKeys.foreach(w => {
        counts(w) = counts.getOrElse(w, 0) + 1
      })
    }
    // prune keys that appear more than maxBlockSize times
    val boundedKeys = new HashSet[String]
    boundedKeys ++= keys.filter(counts(_) <= maxBlockSize)
    boundedKeys
  }

  def getValidKeys: HashSet[String] = validKeys

  def getValidKeysFor(m: Mention) = (if (m.isRecord) recordExtractor(m) else textExtractor(m)).filter(validKeys(_))

  private def initInvertedIndex(): HashMap[String, Seq[String]] = {
    val invIndex = new HashMap[String, Seq[String]]
    for (m <- mentions) {
      val mentionKeys = getValidKeysFor(m)
      mentionKeys.foreach(key => {
        invIndex(key) = invIndex.getOrElse(key, Seq.empty[String]) ++ Seq(m.id)
      })
    }
    invIndex
  }

  def getNeighborIds(m: Mention): Seq[String] = {
    val mentionKeys = getValidKeysFor(m)
    var nbrIds = new ArrayBuffer[String]
    mentionKeys.foreach(key => nbrIds ++= invertedIndex(key))
    nbrIds.toSeq
  }

  private def initPairs(): HashSet[(String, String)] = {
    val idPairs = new HashSet[(String, String)]
    for (m <- mentions) {
      val nbrIds = getNeighborIds(m)
      nbrIds.foreach(id => idPairs += orderedIds(m.id, id))
    }
    idPairs
  }

  def numPairs: Int = allPairs.size

  def isPair(id1: String, id2: String): Boolean = allPairs.contains(orderedIds(id1, id2))

  def getAllPairs = allPairs
}