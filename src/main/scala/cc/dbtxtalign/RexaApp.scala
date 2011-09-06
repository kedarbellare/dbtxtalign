package cc.dbtxtalign

import blocking.{AbstractBlocker, PhraseHash, InvertedIndexBlocker, UnionIndexBlocker}
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author kedar
 */


object RexaApp extends AbstractAlign {
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
    println("#author1Pairs=" + authorIndex1.numPairs + " recall=" + authorIndex1.getRecall(cluster2ids, id2mention))
    println("#title1Pairs=" + titleIndex1.numPairs + " recall=" + titleIndex1.getRecall(cluster2ids, id2mention))
    println("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, id2mention, false))

    unionIndex1
  }

  def main(args: Array[String]) {
    val rawRecords = FileHelper.getRawMentions(true, args(0))
    val rawTexts = FileHelper.getRawMentions(false, args(1))
    val rawMentions = rawRecords ++ rawTexts
    println("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    // val id2example = new HashMap[String, FeatMentionExample]
    var numMentions = 0
    val maxMentions = rawMentions.size
    for (m <- rawMentions) {
      // val featSeq = m.words.map(featureIndexer.indexOf_!(_))
      val trueSeg = getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths)
      id2mention(m.id) = m
      // id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, featSeq, trueSeg)
      numMentions += 1
      if (numMentions % 1000 == 0) println("Processed " + numMentions + "/" + maxMentions)
    }

    val id2cluster = FileHelper.getMentionClusters(args(2))
    val cluster2ids = getClusterToIds(id2cluster)

    // 1. calculate candidate pairs using author and title
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    println("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))
  }
}