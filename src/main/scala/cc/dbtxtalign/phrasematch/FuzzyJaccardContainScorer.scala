package cc.dbtxtalign.phrasematch

import collection.mutable.HashSet

/**
 * @author kedar
 */


class FuzzyJaccardContainScorer(tokenMatcher: (String, String) => Boolean =
                         ObjectStringScorer.getJaroWinklerScore(_, _) >= 0.9)
  extends AStringScorer {
  protected def getMatchedToken(w: String, bag: HashSet[String]): Option[String] = {
    bag.foreach(ow => {
      if (tokenMatcher(w, ow)) return Some(ow)
    })
    None
  }

  def setSimilarity(bag1: HashSet[String], bag2: HashSet[String]): Double = {
    val size1 = bag1.size
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
    if (size1 == 0) 0.0
    else numIntersection / size1
  }

  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    setSimilarity(
      getTokensUniqueBag(getTokens(phrase1)),
      getTokensUniqueBag(getTokens(phrase2)))
  }
}