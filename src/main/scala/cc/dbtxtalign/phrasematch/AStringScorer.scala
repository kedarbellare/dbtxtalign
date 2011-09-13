package cc.dbtxtalign.phrasematch

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler
import collection.mutable.{HashSet, HashMap, ArrayBuffer}

/**
 * @author kedar
 */


trait AStringScorer {
  val jwink = new JaroWinkler

  def getJaroWinklerScore(t1: String, t2: String): Double = {
    val score = jwink.getSimilarity(t1, t2)
    if (!score.isInfinite && !score.isNaN) score else 0.0
  }

  def getTokens(phrase: Seq[String], ignoreCase: Boolean = true, ignorePunctuation: Boolean = true): Seq[String] = {
    val buff = new ArrayBuffer[String]
    phrase.foreach(w => {
      val w1 = if (ignoreCase) w.toLowerCase() else w
      val w2 = w1.replaceAll("[^A-Za-z0-9]+", " ").trim()
      if (w2.length() > 0) buff ++= w2.split("\\s+")
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