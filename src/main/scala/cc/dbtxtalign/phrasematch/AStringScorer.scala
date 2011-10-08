package cc.dbtxtalign.phrasematch

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler
import collection.mutable.{HashSet, HashMap, ArrayBuffer}

/**
 * @author kedar
 */

trait AStringScorer {
  val jwink = new JaroWinkler

  def getThresholdLevenshtein(_s: String, _t: String, threshold: Int = 3): Int = {
    val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
    val slen = s.length
    val tlen = t.length

    var prev = Array.fill[Int](tlen + 1)(Int.MaxValue)
    var curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    for (n <- 0 until math.min(tlen + 1, threshold + 1)) prev(n) = n

    for (row <- 1 until (slen + 1)) {
      curr(0) = row
      val min = math.min(tlen + 1, math.max(1, row - threshold))
      val max = math.min(tlen + 1, row + threshold + 1)

      if (min > 1) curr(min - 1) = Int.MaxValue
      for (col <- min until max) {
        curr(col) = if (s(row - 1) == t(col - 1)) prev(col - 1)
        else math.min(prev(col - 1), math.min(curr(col - 1), prev(col))) + 1
      }
      prev = curr
      curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    }

    prev(tlen)
  }

  def getJaroWinklerScore(t1: String, t2: String): Double = {
    val score = jwink.getSimilarity(t1, t2)
    if (!score.isInfinite && !score.isNaN) score else 0.0
  }

  def getTokens(phrase: Seq[String], ignoreCase: Boolean = true, ignorePunctuation: Boolean = true): Seq[String] = {
    val buff = new ArrayBuffer[String]
    phrase.foreach(w => {
      val w1 = if (ignoreCase) w.toLowerCase() else w
      val w2 = if (ignorePunctuation) w1.replaceAll("^[^A-Za-z0-9]+", "").replaceAll("[^A-Za-z0-9]+$", "") else w1
      if (w2.length() > 0) buff += w1
    })
    buff.toSeq
  }

  def getNgramTokens(phrase: Seq[String], n: Int, ignoreCase: Boolean = true, ignorePunctuation: Boolean = true): Seq[String] = {
    val buff = new ArrayBuffer[String]
    phrase.foreach(w => {
      val w1 = if (ignoreCase) w.toLowerCase() else w
      val w2 = if (ignorePunctuation) w1.replaceAll("^[^A-Za-z0-9]+", "").replaceAll("[^A-Za-z0-9]+$", "") else w1
      if (w2.length() > 0) {
        val w3 = "^" + w1 + "$"
        for (i <- 0 until w3.length() - n) {
          buff += w3.slice(i, i + n).intern()
        }
      }
    })
    buff.toSeq
  }

  def getTokensBag(tokens: Seq[String]): HashMap[String, Double] = {
    val bag = new HashMap[String, Double]
    tokens.foreach(w => {
      bag(w) = bag.getOrElse(w, 0.0) + 1
    })
    bag
  }

  def getTokensNormalizedBag(bag: HashMap[String, Double]): HashMap[String, Double] = {
    val normbag = new HashMap[String, Double]
    var normalizer = 0.0
    bag.values.foreach(wt => {
      normalizer += wt * wt
    })
    normalizer = math.sqrt(normalizer)
    if (normalizer > 0) bag.keys.foreach(w => {
      normbag(w) = bag(w) / normalizer
    })
    normbag
  }

  def getTokensUniqueBag(tokens: Seq[String]): HashSet[String] = {
    val uniqtoks = new HashSet[String]
    uniqtoks ++= tokens
    uniqtoks
  }

  def score(phrase1: Seq[String], phrase2: Seq[String]): Double
}