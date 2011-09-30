package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import phrasematch._
import mongo.KB
import collection.mutable.{ArrayBuffer, HashMap}
import cc.refectorie.user.kedarb.dynprog.types.FtrVec

/**
 * @author kedar
 */

object BFTKB extends KB("bft",
  DBAlignConfig.get[String]("mongoHostname", "localhost"),
  DBAlignConfig.get[Int]("mongoPort", 27017))

trait ABFTAlign extends AbstractAlign {
  val kb = BFTKB

  val recordsColl = kb.getColl("records")
  val textsColl = kb.getColl("texts")
  val featuresColl = kb.getColl("features")
  val featureVectorsColl = kb.getColl("featureVectors")

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

  def toFeatExample(m: Mention) = {
    val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => true)
    val features = featuresColl.findOneByID(m.id).get.as[BasicDBList]("features").toArray.map(_.toString)
    new FeatMentionExample(m.id, m.isRecord, m.words, possibleEnds,
      features.map(wordFeatureIndexer.indexOf_!(_)), getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths))
  }

  def toFeatVecExample(m: Mention) = {
    val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => true)
    val featureVectors = featureVectorsColl.findOneByID(m.id).get.as[BasicDBList]("featureVectors").toArray
      .map(_.asInstanceOf[BasicDBList].toArray.map(_.toString))
    val featVecSeq = mapIndex(m.words.length, (ip: Int) => {
      val fv = new FtrVec
      featureVectors(ip).map(featureIndexer.indexOf_!(_)).filter(_ >= 0).toSet.foreach({
        f: Int => fv += f -> 1.0
      })
      fv
    })
    new FeatVecMentionExample(m.id, m.isRecord, m.words, possibleEnds,
      featVecSeq, getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths))
  }

  def getBlocker(rawMentions: Seq[Mention], id2mention: HashMap[String, Mention],
                 cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker = {
    val nameIndex1 = new InvertedIndexBlocker(250, rawMentions, {
      m: Mention => PhraseHash.ngramsWordHash(m.extractTrueWordsFor("hotelname"), Seq(1, 2))
    }, {
      m: Mention => PhraseHash.ngramsWordHash(m.words, Seq(1, 2))
    })
    val nameIndex2 = new InvertedIndexBlocker(25, rawMentions, {
      m: Mention => PhraseHash.ngramsCharHash(m.extractTrueWordsFor("hotelname").mkString(" "), Seq(4, 5, 6))
    }, {
      m: Mention => PhraseHash.ngramsCharHash(m.words.mkString(" "), Seq(4, 5, 6))
    })
    val areaIndex1 = new InvertedIndexBlocker(250, rawMentions, {
      m: Mention => PhraseHash.ngramsWordHash(m.extractTrueWordsFor("localarea"), Seq(1, 2))
    }, {
      m: Mention => PhraseHash.ngramsWordHash(m.words, Seq(1, 2))
    })
    val areaIndex2 = new InvertedIndexBlocker(50, rawMentions, {
      m: Mention => PhraseHash.ngramsCharHash(m.extractTrueWordsFor("localarea").mkString(" "), Seq(4, 5, 6))
    }, {
      m: Mention => PhraseHash.ngramsCharHash(m.words.mkString(" "), Seq(4, 5, 6))
    })
    val unionIndex1 = new UnionIndexBlocker(Seq(nameIndex1, nameIndex2, areaIndex1, areaIndex2), true)

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
    recordsColl.dropCollection()
    textsColl.dropCollection()

    FileHelper.loadRawMentions(kb, true, args(0), "records")
    FileHelper.loadRawMentions(kb, false, args(1), "texts")
  }
}

object BFTFeatureSequenceLoader extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
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

object BFTFeatureVectorSequenceLoader extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
    featureVectorsColl.dropCollection()
    val word2path = FileHelper.getMapping2to1(args(0))
    for (dbo <- kb.getColl("records").find() ++ kb.getColl("texts").find(); m = new Mention(dbo)) {
      val builder = MongoDBObject.newBuilder
      val features = mapIndex(m.words.length, (ip: Int) => {
        val word = m.words(ip)
        val feats = new ArrayBuffer[String]
        feats += "SIMPLIFIED=" + simplify(word)
        if (word.matches("\\d+(\\.\\d+)?\\*")) feats += "CONTAINS_STAR_PATTERN"
        if (word2path.contains(word)) feats += "PATH=" + word2path(word)
        feats.toSeq
      })
      builder += "_id" -> m.id
      builder += "featureVectors" -> features
      featureVectorsColl += builder.result()
      // output features
      if (!m.isRecord) {
        println(features.mkString(" "))
      }
    }
  }
}

object BFTApp extends ABFTAlign with HasLogger {
  def main(args: Array[String]) {
    val rawRecords = recordsColl.map(new Mention(_)).toArray
    val rawTexts = textsColl.map(new Mention(_)).toArray
    val rawMentions = rawRecords ++ rawTexts
    logger.info("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2cluster = FileHelper.getMapping1to2(args(0))
    val cluster2ids = getClusterToIds(id2cluster)
    for (m <- rawMentions) {
      id2mention(m.id) = m
    }

    val fExamples = rawMentions.map(toFeatExample(_))
    val fvecExamples = rawMentions.map(toFeatVecExample(_))

    // 1. Calculate candidate pairs using hotelname and localarea
    val blocker = getBlocker(rawMentions, id2mention, cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))
    logger.info("#recordPairsMatched=" + getNumRecordPairsMatched(rawRecords, blocker))

    // 3. Segment HMM baseline
    var hmmParams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    hmmParams.setUniform_!
    hmmParams.normalize_!(1e-2)
    hmmParams = learnEMSegmentParamsHMM(20, fExamples, hmmParams, 1e-2, 1e-2)
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

    val hplExamples = getHighPrecisionLabeledExamples(fvecExamples, blocker, approxMatchers,
      approxMatchThresholds, approxMatchThresholds.foldLeft(0.0)(_ + _), id2cluster)
    var crfParams = newSegmentParams(false, true, labelIndexer, featureIndexer)
    crfParams.setUniform_!
    crfParams = learnSupervisedSegmentParamsCRF(50, hplExamples, crfParams, 1, 1)
    // crfParams.output(logger.info(_))
    decodeSegmentParamsCRF("bft.crf.true.txt", "bft.crf.pred.txt", fvecExamples.filter(_.isRecord == false), crfParams)
  }
}