package cc.dbtxtalign.phrasematch

import collection.mutable.HashSet

/**
 * @author kedar
 */


object JaccardContainScorer extends AStringScorer {
  def score(bag1: HashSet[String], bag2: HashSet[String]): Double = {
    val numIntersection = 1.0 * bag1.filter(bag2(_)).size
    if (bag1.size == 0) 0.0
    else numIntersection / bag1.size
  }

  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    score(
      getTokensUniqueBag(getTokens(phrase1)),
      getTokensUniqueBag(getTokens(phrase2)))
  }
}