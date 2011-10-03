package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


class SoftJaccardContainScorer(val tokenMatchThreshold: Double = 0.9) extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    val bag1 = getTokensUniqueBag(getTokens(phrase1))
    val bag2 = getTokensUniqueBag(getTokens(phrase2))
    val bag1Size = 1.0 * bag1.size
    var numIntersection = 0.0
    bag1.foreach(w => {
      if (bag2(w)) {
        numIntersection += 1
        bag2.remove(w)
      } else {
        var bestMatchTok: String = null
        var bestMatchScore = Double.NegativeInfinity
        bag2.foreach(ow => {
          val matchScore = getJaroWinklerScore(w, ow)
          if (matchScore > bestMatchScore && matchScore >= tokenMatchThreshold) {
            bestMatchTok = ow
            bestMatchScore = matchScore
          }
        })
        if (bestMatchScore >= tokenMatchThreshold) {
          numIntersection += bestMatchScore
          bag2.remove(bestMatchTok)
        }
      }
    })
    if (bag1Size == 0) 0.0
    else numIntersection / bag1Size
  }
}