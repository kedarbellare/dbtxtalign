package cc.dbtxtalign.phrasematch

/**
 * @author kedar
 */


class NoopScorer(val defaultScore: Double = 0.0) extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = defaultScore
}