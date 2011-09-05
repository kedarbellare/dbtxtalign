package cc.dbtxtalign.blocking

import collection.mutable.HashSet

/**
 * @author kedar
 */


class UnionIndexBlocker(val indices: Seq[InvertedIndexBlocker], val useOr: Boolean) extends AbstractBlocker {
  private val allPairs = new HashSet[(String, String)]
  if (useOr) indices.foreach(idx => allPairs ++= idx.getAllPairs)
  else if (indices.size > 0) {
    val index1Pairs = indices(0).getAllPairs
    if (indices.size > 1) {
      val restIndices = indices.drop(1)
      allPairs ++= index1Pairs.filter(idpair => restIndices.forall(_.isPair(idpair._1, idpair._2) == true))
    } else allPairs ++= index1Pairs
  }

  def numPairs = allPairs.size

  def getAllPairs = allPairs

  def isPair(id1: String, id2: String) = allPairs.contains(orderedIds(id1, id2))
}