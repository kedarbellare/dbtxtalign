package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils
import collection.mutable.HashMap
import org.riedelcastro.nurupo.HasLogger
import blocking.{AbstractBlocker, PhraseHash, InvertedIndexBlocker, UnionIndexBlocker}
import mongo.KB

/**
 * @author kedar
 */

object RexaKB extends KB("rexa",
  DBAlignConfig.get[String]("mongoHostname", "localhost"),
  DBAlignConfig.get[Int]("mongoPort", 27017))

trait ARexaAlign extends AbstractAlign {
  val kb = RexaKB

  val YEAR = "(19|20)\\d\\d[a-z]?"
  val REFMARKER = "\\[[A-Za-z]*\\d+\\]"
  val INITIALS = "[A-Z]\\."
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"

  def simplify(s: String): String = {
    if (s.matches(YEAR)) "$year$"
    else if (s.matches(REFMARKER)) "$refmarker$"
    else if (s.matches(INITIALS)) "$initials$"
    else if (s.toLowerCase.matches(MONTH)) "$month$"
    else if (s.toLowerCase.matches(DOTW)) "$day$"
    else if (s.matches("\\(" + YEAR + "\\)")) "$yearbraces$"
    else s.replaceAll("\\d", "0").toLowerCase
  }

  def isPossibleEnd(j: Int, words: Seq[String]): Boolean = {
    val endsOnPunc = "^.*[^A-Za-z0-9\\-]$"
    val startsWithPunc = "^[^A-Za-z0-9\\-].*$"
    val endsWithAlpha = "^.*[A-Za-z]$"
    val endsWithNum = "^.*[0-9]$"
    val startsWithAlpha = "^[A-Za-z].*$"
    val startsWithNum = "^[0-9].*$"
    val endsOnSpecial = "^(and|et\\.?|vol\\.?|no\\.?|pp\\.?|pages)$"
    // info("calling isEnd('" + words(j - 1) + "'): " + words.mkString(" "))
    if (j == 0) false
    else j == words.length ||
      words(j - 1).matches(endsOnPunc) || // word ends on punctuation
      words(j).matches(startsWithPunc) || // words begins with punctuation
      words(j - 1).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      words(j).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      (words(j - 1).matches(endsWithAlpha) && words(j).matches(startsWithNum)) || // alpha -> num
      (words(j - 1).matches(endsWithNum) && words(j).matches(startsWithAlpha)) // num -> alpha
  }

  def getBlocker(rawMentions: Seq[Mention], id2mention: HashMap[String, Mention],
                 cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker = {
    val authorIndex1 = new InvertedIndexBlocker(500, rawMentions, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("author"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    })
    val titleIndex1 = new InvertedIndexBlocker(500, rawMentions, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("title"), 2)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 2)
    })
    val unionIndex1 = new UnionIndexBlocker(Seq(authorIndex1, titleIndex1), false)

    // recall of hashes
    logger.info("#author1Pairs=" + authorIndex1.numPairs + " recall=" + authorIndex1.getRecall(cluster2ids, id2mention))
    logger.info("#title1Pairs=" + titleIndex1.numPairs + " recall=" + titleIndex1.getRecall(cluster2ids, id2mention))
    logger.info("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, id2mention, false))

    unionIndex1
  }
}

object RexaMongoLoader extends ARexaAlign with HasLogger {
  def main(args: Array[String]) {
    kb.getColl("records").drop()
    kb.getColl("texts").drop()

    FileHelper.loadRawMentions(kb, true, args(0), "records")
    FileHelper.loadRawMentions(kb, false, args(1), "texts")
  }
}

object RexaApp extends ARexaAlign with HasLogger {
  def main(args: Array[String]) {
    val rawRecords = FileHelper.getRawMentions(true, args(0))
    val rawTexts = FileHelper.getRawMentions(false, args(1))
    val rawMentions = rawRecords ++ rawTexts
    println("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2example = new HashMap[String, FeatMentionExample]
    var numMentions = 0
    val maxMentions = rawMentions.size
    for (m <- rawMentions) {
      val featSeq = getFeatureSequence_!(m, wordFeatureIndexer, simplify(_))
      val trueSeg = getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths)
      val possibleEnds = Utils.mapIndex(m.words.length + 1, (j: Int) => RexaApp.isPossibleEnd(j, m.words))
      id2mention(m.id) = m
      id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, possibleEnds, featSeq, trueSeg)
      numMentions += 1
      if (numMentions % 1000 == 0) logger.info("Processed " + numMentions + "/" + maxMentions)
    }

    val id2cluster = FileHelper.getMapping1to2(args(2))
    val cluster2ids = getClusterToIds(id2cluster)
    val examples = id2example.values.toSeq

    // 1. calculate candidate pairs using author and title
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))

    // 3. Segment HMM baseline
    val segparams = learnEMSegmentParamsHMM(20, examples, 1e-2, 1e-2)
    decodeSegmentParamsHMM("rexa.hmm.true.txt", "rexa.hmm.pred.txt", examples, segparams)
  }
}