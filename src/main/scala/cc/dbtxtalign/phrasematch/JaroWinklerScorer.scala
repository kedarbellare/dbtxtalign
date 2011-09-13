package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


object JaroWinklerScorer extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    val s1 = getTokens(phrase1).mkString(" ")
    val s2 = getTokens(phrase2).mkString(" ")
    getJaroWinklerScore(s1, s2)
  }
}