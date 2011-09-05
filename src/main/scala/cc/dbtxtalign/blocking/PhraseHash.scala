package cc.dbtxtalign.blocking

import collection.mutable.HashSet

/**
 * @author kedar
 * @since 3/25/11
 */

object PhraseHash {
  val _noHash = new HashSet[String]

  def noHash(phrase: Seq[String]): HashSet[String] = _noHash

  def ngramWordHash(phrase: Seq[String], n: Int): HashSet[String] = {
    val grams = new HashSet[String]
    val buff = new StringBuffer()
    for (i <- (1 - n) until phrase.length) {
      buff.setLength(0)
      for (j <- i until (i + n)) {
        // jth word
        if (j < 0) buff.append("$begin_").append(j).append('$')
        else if (j >= phrase.length) buff.append("$end_+").append(j - phrase.length + 1).append('$')
        else buff.append(phrase(j))
        // separator
        buff.append('#')
      }
      grams += buff.toString.toLowerCase
    }
    grams
  }

  def ngramCharHash(phrase: Seq[String], n: Int): HashSet[String] = {
    val grams = new HashSet[String]
    phrase.foreach(w => {
      grams ++= ngramWordHash(w.map(_.toString), n)
    })
    grams
  }
}