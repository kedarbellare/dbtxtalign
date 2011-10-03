package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import params.Params
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

  def getBlocker(cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker = {
    val nameIndex1 = new InvertedIndexBlocker(250, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramsWordHash(m.extractTrueWordsFor("hotelname"), Seq(1, 2))
    }, {
      m: Mention => PhraseHash.ngramsWordHash(m.words, Seq(1, 2))
    })
    val nameIndex2 = new InvertedIndexBlocker(25, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramsCharHash(m.extractTrueWordsFor("hotelname").mkString(" "), Seq(4, 5, 6))
    }, {
      m: Mention => PhraseHash.ngramsCharHash(m.words.mkString(" "), Seq(4, 5, 6))
    })
    val areaIndex1 = new InvertedIndexBlocker(250, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramsWordHash(m.extractTrueWordsFor("localarea"), Seq(1, 2))
    }, {
      m: Mention => PhraseHash.ngramsWordHash(m.words, Seq(1, 2))
    })
    val areaIndex2 = new InvertedIndexBlocker(50, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramsCharHash(m.extractTrueWordsFor("localarea").mkString(" "), Seq(4, 5, 6))
    }, {
      m: Mention => PhraseHash.ngramsCharHash(m.words.mkString(" "), Seq(4, 5, 6))
    })
    val unionIndex1 = new UnionIndexBlocker(Seq(nameIndex1, nameIndex2, areaIndex1, areaIndex2), true)

    // recall of hash1
    logger.info("#name1Pairs=" + nameIndex1.numPairs + " recall=" + nameIndex1.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#name2Pairs=" + nameIndex2.numPairs + " recall=" + nameIndex2.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#area1Pairs=" + areaIndex1.numPairs + " recall=" + areaIndex1.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#area2Pairs=" + areaIndex2.numPairs + " recall=" + areaIndex2.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, recordsColl, textsColl, true))

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
  val hotelNameIndex = labelIndexer.indexOf_!("hotelname")
  val localAreaIndex = labelIndexer.indexOf_!("localarea")
  val starRatingIndex = labelIndexer.indexOf_!("starrating")

  val BIAS_MATCH = "bias_match"
  val CHAR2_JACCARD = "char_jaccard[n=2]"
  val CHAR3_JACCARD = "char_jaccard[n=3]"
  val FUZZY_JACCARD_CONTAINS = "fuzzy_jaccard_contains"
  val SOFT_JACCARD70_CONTAINS = "soft_jaccard_contains[>=0.70]"
  val SOFT_JACCARD85_CONTAINS = "soft_jaccard_contains[>=0.85]"
  val SOFT_JACCARD90_CONTAINS = "soft_jaccard_contains[>=0.90]"
  val SOFT_JACCARD95_CONTAINS = "soft_jaccard_contains[>=0.95]"
  val JACCARD_CONTAINS = "jaccard_contains"
  val CHAR2_JACCARD_CONTAINS = "char_jaccard_contains[n=2]"
  val SIM_BINS = Seq(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0)

  alignFeatureIndexer += BIAS_MATCH
  for (s <- SIM_BINS) {
    alignFeatureIndexer += (CHAR2_JACCARD + ">=" + s)
    alignFeatureIndexer += (CHAR3_JACCARD + ">=" + s)
    alignFeatureIndexer += (FUZZY_JACCARD_CONTAINS + ">=" + s)
    alignFeatureIndexer += (SOFT_JACCARD70_CONTAINS + ">=" + s)
    alignFeatureIndexer += (SOFT_JACCARD85_CONTAINS + ">=" + s)
    alignFeatureIndexer += (SOFT_JACCARD90_CONTAINS + ">=" + s)
    alignFeatureIndexer += (SOFT_JACCARD95_CONTAINS + ">=" + s)
    alignFeatureIndexer += (JACCARD_CONTAINS + ">=" + s)
    alignFeatureIndexer += (CHAR2_JACCARD_CONTAINS + ">=" + s)
  }

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
    val blocker = getBlocker(cluster2ids)

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
      if (l == otherLabelIndex) new NoopScorer(0).score(_, _)
      else if (l == hotelNameIndex) new FuzzyJaccardScorer(approxTokenMatcher).score(_, _)
      else if (l == localAreaIndex) new FuzzyJaccardScorer(approxTokenMatcher).score(_, _)
      else new SoftJaccardScorer(0.95).score(_, _)
    })
    val approxMatchThresholds = mapIndex(L, (l: Int) => {
      if (l == otherLabelIndex) 0.0
      else if (l == hotelNameIndex) 0.9
      else if (l == localAreaIndex) 0.9
      else 0.99
    })

    val hplExamples = getHighPrecisionLabeledExamples(fvecExamples, blocker, approxMatchers,
      approxMatchThresholds, approxMatchThresholds.foldLeft(0.0)(_ + _), id2cluster)
    var crfParams = newSegmentParams(false, true, labelIndexer, featureIndexer)
    crfParams.setUniform_!
    crfParams = learnSupervisedSegmentParamsCRF(50, hplExamples, crfParams, 1, 1)
    // crfParams.output(logger.info(_))
    decodeSegmentParamsCRF("bft.crf.true.txt", "bft.crf.pred.txt", fvecExamples.filter(_.isRecord == false), crfParams)

    // 5. Do alignment using gold-standard clusters
    def alignFeaturizer(l: Int, phrase: Seq[String], otherPhrase: Seq[String]): FtrVec = {
      val fv = new FtrVec
      val charJacc2 = new CharJaccardScorer(2).score(phrase, otherPhrase)
      val charJacc3 = new CharJaccardScorer(3).score(phrase, otherPhrase)
      val fuzzyJacc = new FuzzyJaccardContainScorer(approxTokenMatcher).score(phrase, otherPhrase)
      val softJacc70 = new SoftJaccardContainScorer(0.70).score(phrase, otherPhrase)
      val softJacc85 = new SoftJaccardContainScorer(0.85).score(phrase, otherPhrase)
      val softJacc90 = new SoftJaccardContainScorer(0.90).score(phrase, otherPhrase)
      val softJacc95 = new SoftJaccardContainScorer(0.95).score(phrase, otherPhrase)
      val jaccContain = JaccardContainScorer.score(phrase, otherPhrase)
      val charJaccContain = new CharJaccardContainScorer(2).score(phrase, otherPhrase)
      fv += alignFeatureIndexer.indexOf_?(BIAS_MATCH) -> 1.0
      for (sim <- SIM_BINS) {
        if (charJacc2 >= sim) fv += alignFeatureIndexer.indexOf_?(CHAR2_JACCARD + ">=" + sim) -> 1.0
        if (charJacc3 >= sim) fv += alignFeatureIndexer.indexOf_?(CHAR3_JACCARD + ">=" + sim) -> 1.0
        if (fuzzyJacc >= sim) fv += alignFeatureIndexer.indexOf_?(FUZZY_JACCARD_CONTAINS + ">=" + sim) -> 1.0
        if (softJacc70 >= sim) fv += alignFeatureIndexer.indexOf_?(SOFT_JACCARD70_CONTAINS + ">=" + sim) -> 1.0
        if (softJacc85 >= sim) fv += alignFeatureIndexer.indexOf_?(SOFT_JACCARD85_CONTAINS + ">=" + sim) -> 1.0
        if (softJacc90 >= sim) fv += alignFeatureIndexer.indexOf_?(SOFT_JACCARD90_CONTAINS + ">=" + sim) -> 1.0
        if (softJacc95 >= sim) fv += alignFeatureIndexer.indexOf_?(SOFT_JACCARD95_CONTAINS + ">=" + sim) -> 1.0
        if (jaccContain >= sim) fv += alignFeatureIndexer.indexOf_?(JACCARD_CONTAINS + ">=" + sim) -> 1.0
        if (charJaccContain >= sim) fv += alignFeatureIndexer.indexOf_?(CHAR2_JACCARD_CONTAINS + ">=" + sim) -> 1.0
      }
      fv
    }

    val alignFvecExamples = new ArrayBuffer[FeatVecAlignmentMentionExample]
    var maxDegree = 0
    for (m1 <- rawMentions) {
      val clustOpt1 = id2cluster.get(m1.id)
      val ex = toFeatVecExample(m1)
      var degree = 0
      for (m2 <- rawRecords if blocker.isPair(m1.id, m2.id) && m1.id != m2.id) {
        val clust2 = id2cluster(m2.id)
        val isMatch = clustOpt1.isDefined && clustOpt1.get == clust2
        degree += 1
        alignFvecExamples += new FeatVecAlignmentMentionExample(ex.id, ex.isRecord, ex.words, ex.possibleEnds,
          ex.featSeq, isMatch, ex.trueSegmentation, m2.id, m2.words, getSegmentation_!(m2, labelIndexer))
      }
      if (degree > maxDegree) maxDegree = degree
    }
    logger.info("#alignExamples=" + alignFvecExamples.size +
      " #alignMatchExamples=" + alignFvecExamples.filter(_.trueMatch == true).size +
      " maxDegree=" + maxDegree)

    var params = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    params.setUniform_!
    // params = new Params(crfParams.transitions, crfParams.emissions, params.aligns)
    params = learnSupervisedAlignParamsCRF(50, alignFvecExamples, params, alignFeaturizer, 1, 1)
    params.output(logger.info(_))
  }
}