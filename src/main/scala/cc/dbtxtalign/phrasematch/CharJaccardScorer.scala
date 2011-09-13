package cc.dbtxtalign.phrasematch

import cc.dbtxtalign.blocking.PhraseHash

/**
 * @author kedar
 */


class CharJaccardScorer(val n: Int = 2) extends AStringScorer {
  def score(phrase1: Seq[String], phrase2: Seq[String]) = {
    JaccardScorer.score(
      PhraseHash.ngramCharHash(getTokens(phrase1), n),
      PhraseHash.ngramCharHash(getTokens(phrase2), n))
  }
}