package cc.dbtxtalign.fastsim

import collection.mutable.HashMap
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.dbtxtalign.params.HashWeightVec
import cc.refectorie.user.kedarb.dynprog.types.Indexer

/**
 * @author kedarb
 * @since 10/5/11
 */

class SimilarityIndex(val tokenizer: (String) => Seq[String]) {
  type CountVec = HashWeightVec[Int]

  // token -> index
  private val _tokenIndexer = new Indexer[String]
  // id -> (offset -> (token -> weight))
  private val _index = new HashMap[String, Seq[CountVec]]

  def index(id: String, words: Seq[String]) {
    _index(id) = words.map(w => {
      val vec = new CountVec()
      tokenizer(w).map(_tokenIndexer.indexOf_!(_)).foreach(t => vec.increment_!(t, 1))
      vec
    })
  }

  private def getVector(id: String, i: Int, j: Int): CountVec = {
    val vecs = _index(id)
    val vec = new CountVec()
    forIndex(i, j, k => vec.increment_!(vecs(k), 1))
    vec
  }

  def jaccardScore(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int): Double = {
    val vec1 = getVector(id1, i1, j1)
    val vec2 = getVector(id2, i2, j2)
    val intersection = vec1.intersect(vec2)
    val union = vec1.absNorm + vec2.absNorm - intersection
    if (union > 0) intersection / union else 0
  }

  def containsJaccardScore(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int): Double = {
    val vec1 = getVector(id1, i1, j1)
    val vec2 = getVector(id2, i2, j2)
    val union = vec1.absNorm
    if (union > 0) vec1.intersect(vec2) / union else 0
  }

  def cosineScore(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int): Double = {
    val vec1 = getVector(id1, i1, j1).toUnitVector
    val vec2 = getVector(id2, i2, j2).toUnitVector
    vec1.dot(vec2)
  }

  private def approxJoinScorer(_vec1: CountVec, _vec2: CountVec, oper: (Double, Double) => Double,
                               tokenScorer: (String, String) => Double): Double = {
    val (vec1, vec2) = if (_vec1.size < _vec2.size) (_vec1, _vec2) else (_vec2, _vec1)
    var intersection = 0.0
    for ((key, value) <- vec1) {
      if (vec2.contains(key)) {
        intersection += oper(value, vec2(key))
        vec2.remove(key)
      } else {
        val keyStr = _tokenIndexer(key)
        var bestScore = 0.0
        var bestKey = -1
        for (okey <- vec2.keys; okeyStr = _tokenIndexer(okey)) {
          val score =
            if (keyStr.hashCode() < okeyStr.hashCode()) tokenScorer(keyStr, okeyStr) else tokenScorer(okeyStr, keyStr)
          if (score > bestScore) {
            bestScore = score
            bestKey = okey
          }
        }
        if (bestScore > 0 && bestKey >= 0) {
          intersection += bestScore * oper(value, vec2(bestKey))
          vec2.remove(bestKey)
        }
      }
    }
    intersection
  }

  def approxJaccardScorer(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int,
                          tokenScorer: (String, String) => Double): Double = {
    val vec1 = getVector(id1, i1, j1)
    val vec2 = getVector(id2, i2, j2)
    val partialUnion = vec1.absNorm + vec2.absNorm
    val intersection = approxJoinScorer(vec1, vec2, math.min(_, _), tokenScorer)
    if (partialUnion > intersection) intersection / (partialUnion - intersection) else 0
  }

  def approxJaccardContainScorer(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int,
                                 tokenScorer: (String, String) => Double): Double = {
    val vec1 = getVector(id1, i1, j1)
    val vec2 = getVector(id2, i2, j2)
    val partialUnion = vec1.absNorm
    val intersection = approxJoinScorer(vec1, vec2, math.min(_, _), tokenScorer)
    if (partialUnion > 0) intersection / partialUnion else 0
  }

  def approxCosineScorer(id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int,
                         tokenScorer: (String, String) => Double): Double = {
    val vec1 = getVector(id1, i1, j1).toUnitVector
    val vec2 = getVector(id2, i2, j2).toUnitVector
    approxJoinScorer(vec1, vec2, _ * _, tokenScorer)
  }
}