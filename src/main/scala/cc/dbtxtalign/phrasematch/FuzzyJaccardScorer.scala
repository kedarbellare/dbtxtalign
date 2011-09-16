package cc.dbtxtalign.phrasematch

import collection.mutable.HashSet

/**
 * Different from soft jaccard scorer. Considers set intersection based on boolean token match function.
 *
 * @author kedar
 */


class FuzzyJaccardScorer(tokenMatcher: (String, String) => Boolean =
                         ObjectStringScorer.getJaroWinklerScore(_, _) >= 0.9)
  extends AStringScorer {
  protected def getMatchedToken(w: String, bag: HashSet[String]): Option[String] = {
    bag.foreach(ow => {
      if (tokenMatcher(w, ow)) return Some(ow)
    })
    None
  }

  def setSimilarity(_bag1: HashSet[String], _bag2: HashSet[String]): Double = {
    val (bag1, bag2) = if (_bag1.size <= _bag2.size) (_bag1, _bag2) else (_bag2, _bag1)
    val size1 = bag1.size
    val size2 = bag2.size
    var numIntersection = 0.0
    bag1.foreach(w => {
      if (bag2(w)) {
        numIntersection += 1
        bag2.remove(w)
      } else {
        val owOpt = getMatchedToken(w, bag2)
        if (owOpt.isDefined) {
          numIntersection += 1
          bag2.remove(owOpt.get)
        }
      }
    })
    val numUnion = size1 + size2 - numIntersection
    if (numUnion == 0) 0.0
    else numIntersection / numUnion
  }

  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    setSimilarity(
      getTokensUniqueBag(getTokens(phrase1)),
      getTokensUniqueBag(getTokens(phrase2)))
  }
}