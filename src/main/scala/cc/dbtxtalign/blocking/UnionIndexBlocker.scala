package cc.dbtxtalign.blocking

import collection.mutable.HashSet
import org.riedelcastro.nurupo.Counting

/**
 * @author kedar
 */


class UnionIndexBlocker(val indices: Seq[InvertedIndexBlocker], val useOr: Boolean) extends AbstractBlocker {
  private val allPairs = new HashSet[(String, String)]
  if (useOr) indices.foreach(idx => allPairs ++= idx.getAllPairs)
  else if (indices.size > 0) {
    var smallestPairs = indices(0).getAllPairs
    val allIndicesPairs = indices.map(_.getAllPairs)
    allIndicesPairs.foreach(pairs => {
      if (smallestPairs.size > pairs.size)
        smallestPairs = pairs
    })
    if (indices.size > 1) {
      val counting = new Counting(10000, cnt => println("Processed " + cnt + "/" + smallestPairs.size))
      val restIndicesPairs = allIndicesPairs.filter(_ != smallestPairs)
      for (pair <- counting(smallestPairs)) {
        if (restIndicesPairs.forall(_.contains(pair))) {
          allPairs += pair
        }
      }
    } else {
      allPairs ++= smallestPairs
    }
  }

  def numPairs = allPairs.size

  def getAllPairs = allPairs

  def isPair(id1: String, id2: String) = allPairs.contains(orderedIds(id1, id2))
}