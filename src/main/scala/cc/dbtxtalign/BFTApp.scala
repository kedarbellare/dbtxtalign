package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.InferSpec
import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import phrasematch._
import mongo.KB
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author kedar
 */

object BFTKB extends KB("bft",
  DBAlignConfig.get[String]("mongoHostname", "localhost"),
  DBAlignConfig.get[Int]("mongoPort", 27017))

trait ABFTAlign extends AbstractAlign {
  val kb = BFTKB

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
}

object BFTMongoLoader extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
    kb.getColl("records").dropCollection()
    kb.getColl("texts").dropCollection()

    FileHelper.loadRawMentions(kb, true, args(0), "records")
    FileHelper.loadRawMentions(kb, false, args(1), "texts")
  }
}

object BFTFeatureSequenceLoader extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
    val featuresColl = kb.getColl("features")
    featuresColl.dropCollection()
    for (dbo <- kb.getColl("records").find() ++ kb.getColl("texts").find(); m = new Mention(dbo)) {
      val builder = MongoDBObject.newBuilder
      val features = m.words.map(simplify(_))
      builder += "_id" -> m.id
      builder += "features" -> features
      featuresColl += builder.result()
      // output features
      if (!m.isRecord) {
        println(features.mkString(" "))
      }
    }
  }
}

object BFTApp extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
    val rawRecords = kb.getColl("records").map(new Mention(_)).toArray
    val rawTexts = kb.getColl("texts").map(new Mention(_)).toArray
    val rawMentions = rawRecords ++ rawTexts
    logger.info("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2fExample = new HashMap[String, FeatMentionExample]
    val id2fvecExample = new HashMap[String, FeatVecMentionExample]
    val id2cluster = FileHelper.getMapping1to2(args(0))
    val word2path = FileHelper.getMapping2to1(args(1))
    val cluster2ids = getClusterToIds(id2cluster)
    for (m <- rawMentions) {
      val featSeq = getFeatureSequence_!(m, wordFeatureIndexer, simplify(_))
      val featVecSeq = getFeatureVectorSequence_!(m, featureIndexer, (words: Seq[String], ip: Int) => {
        val word = words(ip)
        val feats = new ArrayBuffer[String]
        feats += "SIMPLIFIED=" + simplify(word)
        if (word.matches("\\d+(\\.\\d+)?\\*")) feats += "CONTAINS_STAR_PATTERN"
        if (word2path.contains(word)) feats += "PATH=" + word2path(word)
        feats.toSeq
      })
      val trueSeg = getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths)
      val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => true)
      id2mention(m.id) = m
      id2fExample(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, possibleEnds, featSeq, trueSeg)
      id2fvecExample(m.id) = new FeatVecMentionExample(m.id, m.isRecord, m.words, possibleEnds, featVecSeq, trueSeg)
    }

    val fExamples = id2fExample.values.toSeq
    val fvecExamples = id2fvecExample.values.toSeq

    // 1. Calculate candidate pairs using hotelname and localarea
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))

    // 3. Segment HMM baseline
    val hmmParams = learnEMSegmentParamsHMM(20, fExamples, 1e-2, 1e-2)
    decodeSegmentParamsHMM("bft.hmm.true.txt", "bft.hmm.pred.txt", fExamples, hmmParams)

    // 4. WWT phase1 segment and learn from high-precision segmentations
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
      else if (lbl == "hotelname") 0.9
      else if (lbl == "localarea") 0.9
      else 0.99
    })

    val hplExamples = getHighPrecisionLabeledExamples(fvecExamples, blocker, approxMatchers, approxMatchThresholds, id2cluster)
    val crfParams = learnSupervisedSegmentParamsCRF(50, hplExamples, 1, 1)
    // crfParams.output(logger.info(_))
    decodeSegmentParamsCRF("bft.crf.true.txt", "bft.crf.pred.txt", fvecExamples.filter(_.isRecord == false), crfParams)
  }
}