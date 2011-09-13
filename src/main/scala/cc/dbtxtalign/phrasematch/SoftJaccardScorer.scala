package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


class SoftJaccardScorer(val tokenMatchThreshold: Double = 0.9) extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    val _bag1 = getTokensUniqueBag(getTokens(phrase1))
    val _bag2 = getTokensUniqueBag(getTokens(phrase2))
    val (bag1, bag2) = if (_bag1.size <= _bag2.size) (_bag1, _bag2) else (_bag2, _bag1)
    val bag1Size = 1.0 * bag1.size
    val bag2Size = 1.0 * bag2.size
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
    var numUnion = bag1Size + bag2Size - numIntersection
    if (numUnion == 0) 0.0
    else numIntersection / numUnion
  }
}