package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


class SoftCosineScorer(val tokenMatchThreshold: Double = 0.9) extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    val _bag1 = getTokensNormalizedBag(getTokensBag(getTokens(phrase1)))
    val _bag2 = getTokensNormalizedBag(getTokensBag(getTokens(phrase2)))
    val (bag1, bag2) = if (_bag1.size < _bag2.size) (_bag1, _bag2) else (_bag2, _bag1)
    var score = 0.0
    bag1.keys.foreach(w => {
      if (bag2.contains(w)) {
        score += bag1(w) * bag2(w)
        bag2.remove(w)
      } else {
        var bestMatchTok: String = null
        var bestMatchScore = Double.NegativeInfinity
        bag2.keys.foreach(ow => {
          val matchScore = getJaroWinklerScore(w, ow)
          if (matchScore > bestMatchScore && matchScore >= tokenMatchThreshold) {
            bestMatchTok = ow
            bestMatchScore = matchScore
          }
        })
        if (bestMatchScore >= tokenMatchThreshold) {
          score += bestMatchScore * bag1(w) * bag2(bestMatchTok)
          bag2.remove(bestMatchTok)
        }
      }
    })
    score
  }
}