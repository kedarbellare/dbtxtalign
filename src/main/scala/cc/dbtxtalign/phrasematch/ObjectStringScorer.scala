package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */

object ObjectStringScorer extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = 0.0
}