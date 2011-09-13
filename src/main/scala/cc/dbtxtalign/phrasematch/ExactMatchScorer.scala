package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


object ExactMatchScorer extends AStringScorer {
  override def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    if (JaccardScorer.score(phrase1, phrase2) >= 0.999) 1 else 0
  }
}