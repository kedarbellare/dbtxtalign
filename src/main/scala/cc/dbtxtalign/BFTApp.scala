package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import collection.mutable.HashMap
import org.apache.log4j.Logger
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import phrasematch._
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */

object BFTApp extends AbstractAlign {
  val logger = Logger.getLogger(this.getClass.getSimpleName)

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
    logger.info("#name1Pairs=" + nameIndex1.numPairs + " recall=" + nameIndex1.getRecall(cluster2ids, id2mention))
    logger.info("#name2Pairs=" + nameIndex2.numPairs + " recall=" + nameIndex2.getRecall(cluster2ids, id2mention))
    logger.info("#area1Pairs=" + areaIndex1.numPairs + " recall=" + areaIndex1.getRecall(cluster2ids, id2mention))
    logger.info("#area2Pairs=" + areaIndex2.numPairs + " recall=" + areaIndex2.getRecall(cluster2ids, id2mention))
    logger.info("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, id2mention, true))

    unionIndex1
  }

  def approxTokenMatcher(t1: String, t2: String): Boolean = {
    ObjectStringScorer.getThresholdLevenshtein(t1, t2, 2) <= 1 || ObjectStringScorer.getJaroWinklerScore(t1, t2) >= 0.95
  }

  def main(args: Array[String]) {
    val rawRecords = FileHelper.getRawMentions(true, args(0))
    val rawTexts = FileHelper.getRawMentions(false, args(1))
    val rawMentions = rawRecords ++ rawTexts
    logger.info("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2example = new HashMap[String, FeatMentionExample]
    for (m <- rawMentions) {
      val featSeq = getFeatureSequence_!(m, wordFeatureIndexer, simplify(_))
      val trueSeg = getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths)
      val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => true)
      id2mention(m.id) = m
      id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, possibleEnds, featSeq, trueSeg)
    }

    val id2cluster = FileHelper.getMentionClusters(args(2))
    val cluster2ids = getClusterToIds(id2cluster)
    val examples = id2example.values.toSeq

    // 1. Calculate candidate pairs using hotelname and localarea
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))

    // 3. Segment HMM baseline
    // val segparams = learnEMSegmentParamsHMM(20, examples, 1e-2, 1e-2)
    // decodeSegmentParamsHMM("bft.hmm.true.txt", "bft.hmm.pred.txt", examples, segparams)

    // 4. WWT phase1 segment
    val approxMatchers = mapIndex(L, (l: Int) => {
      val lbl = labelIndexer(l)
      if (lbl == "O") new NoopScorer(0).score(_, _)
      else if (lbl == "hotelname") new FuzzyJaccardScorer(approxTokenMatcher).score(_, _)
      else if (lbl == "localarea") new FuzzyJaccardScorer(approxTokenMatcher).score(_, _)
      else new SoftJaccardScorer(0.95).score(_, _)
    })
    val approxMatchThresholds = mapIndex(L, (l: Int) => {
      val lbl = labelIndexer(l)
      if (lbl == "O") 0.0
      else if (lbl == "hotelname") 0.8
      else if (lbl == "localarea") 0.8
      else 0.99
    })
    val segparams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    val segcounts = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    segparams.setUniform_!
    segparams.normalize_!(1e-2)

    val textExamples = (for (ex <- examples if !ex.isRecord) yield ex).toSeq
    val recordExamples = (for (ex <- examples if ex.isRecord) yield ex).toSeq
    val approxSumMatchThreshold = approxMatchThresholds.foldLeft(0.0)(_ + _)

    println("=====================================================================================")
    for (ex1 <- textExamples) {
      val clust1 = id2cluster.getOrElse(ex1.id, "NULL")
      var foundMatch = false
      for (ex2 <- recordExamples if blocker.isPair(ex1.id, ex2.id)) {
        val ex = new FeatMatchOnlyMentionExample(ex1.id, ex1.isRecord, ex1.words, ex1.possibleEnds, ex1.featSeq,
          ex1.trueSegmentation, ex2.id, ex2.words, ex2.trueSegmentation)
        val hmmSegMatch = new HMMSegmentAndMatchWWT(labelIndexer, maxLengths, approxMatchers, approxMatchThresholds,
          ex, segparams, segcounts, InferSpec(0, 1, false, false, true, false, true, false, 1, 0))
        if (hmmSegMatch.logVZ >= approxSumMatchThreshold) {
          foundMatch = true
          println()
          println("matchScore: " + hmmSegMatch.logVZ + " clusterMatch: " + (clust1 == id2cluster(ex2.id)))
          println("recordWords: " + ex2.words.mkString(" "))
          println("recordSegmentation: " + ex2.trueSegmentation)
          println("words: " + ex1.words.mkString(" "))
          println("matchSegmentation: " + hmmSegMatch.bestWidget)
          println("segmentation: " + ex1.trueSegmentation)
        }
      }
      
      if (foundMatch)
        println("=====================================================================================")
    }
  }
}