package cc.dbtxtalign.blocking

import java.util.Random
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import cc.dbtxtalign.Mention

/**
 * @author kedar
 */

class MentionMinHash(val numHash: Int,
                     val recordExtractor: Mention => Set[String],
                     val textExtractor: Mention => Set[String],
                     val mentions: Seq[Mention]) {
  private val hash = new Array[Int => Int](numHash)
  private val r = new Random()
  private val wordIndexer = new Indexer[String]

  // create hash functions
  for (i <- 0 until numHash) {
    val a = r.nextInt()
    val b = r.nextInt()
    val c = r.nextInt()
    hash(i) = {
      x: Int => math.abs(((a * (x >> 4) + b * x + c) & 131071))
    }
  }

  // initialize vocabulary based on record mentions
  mentions.foreach(m => {
    // add record mentions word to index
    if (m.isRecord) {
      recordExtractor(m).foreach(wordIndexer.indexOf_!(_))
    }
  })
  wordIndexer.lock
  
  def hashCode(mention: Mention): Array[Int] = {
    val words = if (mention.isRecord) recordExtractor(mention) else textExtractor(mention)
    val hashes = Array.ofDim[Int](numHash)
    val validWordIndices = words.map(wordIndexer.indexOf_?(_)).filter(_ >= 0)
    for (i <- 0 until numHash) {
      hashes(i) = Int.MaxValue
      for (wi <- validWordIndices) hashes(i) = math.min(hashes(i), hash(i)(wi))
    }
    hashes
  }
}