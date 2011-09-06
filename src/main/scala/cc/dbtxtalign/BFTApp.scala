package cc.dbtxtalign

import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author kedar
 */

object BFTApp extends AbstractAlign {
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"
  // val ALPHANUM = "[a-z0-9]"
  // val NUM = "[0-9]"
  // val PUNC = "[,\\.;:?!()\"\\-'`]"

  def simplify(s: String): String = {
    if (s.matches("\\d+/\\d+(/\\d+)?")) "$date$"
    else if (s.matches("\\$\\d+(\\.\\d+)?")) "$price$"
    else if (s.matches(DOTW)) "$day$"
    else if (s.matches(MONTH)) "$month$"
    else s
  }

  def getBlocker(rawMentions: Seq[Mention], id2mention: HashMap[String, Mention],
                 cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker = {
    val nameIndex1 = new InvertedIndexBlocker(250, rawMentions, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("hotelname"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    })
    val nameIndex2 = new InvertedIndexBlocker(50, rawMentions, {
      m: Mention => PhraseHash.ngramCharHash(m.extractTrueWordsFor("hotelname"), 3)
    }, {
      m: Mention => PhraseHash.ngramCharHash(m.words, 3)
    })
    val areaIndex1 = new InvertedIndexBlocker(250, rawMentions, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("localarea"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    })
    val areaIndex2 = new InvertedIndexBlocker(50, rawMentions, {
      m: Mention => PhraseHash.ngramCharHash(m.extractTrueWordsFor("localarea"), 3)
    }, {
      m: Mention => PhraseHash.ngramCharHash(m.words, 3)
    })
    val unionIndex1 = new UnionIndexBlocker(Seq(nameIndex1, nameIndex2, areaIndex1), true)

    // recall of hash1
    println("#name1Pairs=" + nameIndex1.numPairs + " recall=" + nameIndex1.getRecall(cluster2ids, id2mention))
    println("#name2Pairs=" + nameIndex2.numPairs + " recall=" + nameIndex2.getRecall(cluster2ids, id2mention))
    println("#area1Pairs=" + areaIndex1.numPairs + " recall=" + areaIndex1.getRecall(cluster2ids, id2mention))
    println("#area2Pairs=" + areaIndex2.numPairs + " recall=" + areaIndex2.getRecall(cluster2ids, id2mention))
    println("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, id2mention, true))

    unionIndex1
  }

  def main(args: Array[String]) {
    val labelIndexer = new Indexer[String]
    val featureIndexer = new Indexer[String]
    val maxLengths = new ArrayBuffer[Int]

    val rawRecords = FileHelper.getRawMentions(true, args(0))
    val rawTexts = FileHelper.getRawMentions(false, args(1))
    val rawMentions = rawRecords ++ rawTexts
    println("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2example = new HashMap[String, FeatMentionExample]
    for (m <- rawMentions) {
      val featSeq = getFeatureSequence_!(m, featureIndexer, simplify(_))
      val trueSeg = getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths)
      id2mention(m.id) = m
      id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, featSeq, trueSeg)
    }

    val id2cluster = FileHelper.getMentionClusters(args(2))
    val cluster2ids = getClusterToIds(id2cluster)

    // 1. Calculate candidate pairs using hotelname and localarea
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    println("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))
  }
}