package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.Imports._
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import fastsim._
import phrasematch._
import mongo.KB
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import cc.refectorie.user.kedarb.dynprog.InferSpec
import collection.mutable.{HashSet, ArrayBuffer, HashMap}

/**
 * @author kedar
 */

object BFTKB extends KB("bft-scratch",
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
}

object BFTMongoLoader extends ABFTAlign {
  def main(args: Array[String]) {
    recordsColl.dropCollection()
    textsColl.dropCollection()

    FileHelper.loadRawMentions(kb, true, args(0), "records")
    FileHelper.loadRawMentions(kb, false, args(1), "texts")
  }
}

object BFTFeatureSequenceLoader extends ABFTAlign {
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

object BFTFeatureVectorSequenceLoader extends ABFTAlign {
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

object BFTApp extends ABFTAlign {
  val hotelNameIndex = labelIndexer.indexOf_!("hotelname")
  val localAreaIndex = labelIndexer.indexOf_!("localarea")
  val starRatingIndex = labelIndexer.indexOf_!("starrating")

  val tokenSimilarityIndex = new SimilarityIndex(s => ObjectStringScorer.getTokens(Seq(s)))
  val bigramSimilarityIndex = new SimilarityIndex(s => ObjectStringScorer.getNgramTokens(Seq(s), 2))
  val trigramSimilarityIndex = new SimilarityIndex(s => ObjectStringScorer.getNgramTokens(Seq(s), 3))

  val BIAS_MATCH = "bias_match"
  val CHAR2_JACCARD = "char_jaccard[n=2]"
  val CHAR2_JACCARD_CONTAINS = "char_jaccard_contains[n=2]"
  val CHAR3_JACCARD = "char_jaccard[n=3]"
  val FUZZY_JACCARD_CONTAINS = "fuzzy_jaccard_contains"
  val FUZZY_JACCARD = "fuzzy_jaccard"
  val SOFT_JACCARD70 = "soft_jaccard[>=0.70]"
  val SOFT_JACCARD85 = "soft_jaccard[>=0.85]"
  val SOFT_JACCARD90 = "soft_jaccard[>=0.90]"
  val SOFT_JACCARD95 = "soft_jaccard[>=0.95]"
  val SOFT_JACCARD70_CONTAINS = "soft_jaccard_contains[>=0.70]"
  val SOFT_JACCARD85_CONTAINS = "soft_jaccard_contains[>=0.85]"
  val SOFT_JACCARD90_CONTAINS = "soft_jaccard_contains[>=0.90]"
  val SOFT_JACCARD95_CONTAINS = "soft_jaccard_contains[>=0.95]"
  val JACCARD = "jaccard"
  val JACCARD_CONTAINS = "jaccard_contains"
  val SIM_BINS = Seq(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0)

  alignFeatureIndexer += BIAS_MATCH
  for (s <- SIM_BINS) {
    addThresholdFeaturesToIndexer(alignFeatureIndexer, CHAR2_JACCARD, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, CHAR2_JACCARD_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, CHAR3_JACCARD, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, FUZZY_JACCARD, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, FUZZY_JACCARD_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD70, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD85, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD90, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD95, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD70_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD85_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD90_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, SOFT_JACCARD95_CONTAINS, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, JACCARD, s)
    addThresholdFeaturesToIndexer(alignFeatureIndexer, JACCARD_CONTAINS, s)
  }

  def approxTokenMatcher(t1: String, t2: String): Double = {
    if (ObjectStringScorer.getThresholdLevenshtein(t1, t2, 2) <= 1 ||
      ObjectStringScorer.getJaroWinklerScore(t1, t2) >= 0.95) 1
    else 0
  }

  def approxJaroWinklerScorer(t1: String, t2: String, threshold: Double = 0.95): Double = {
    val score = ObjectStringScorer.getJaroWinklerScore(t1, t2)
    if (score >= threshold) score else 0
  }

  override def getAlignFeatureVector(l: Int, id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int): FtrVec = {
    val fv = new FtrVec
    val char2Jacc = bigramSimilarityIndex.jaccardScore(id1, i1, j1, id2, i2, j2)
    val char2JaccContains = bigramSimilarityIndex.containsJaccardScore(id1, i1, j1, id2, i2, j2)
    val char3Jacc = trigramSimilarityIndex.jaccardScore(id1, i1, j1, id2, i2, j2)
    val fuzzyJacc = tokenSimilarityIndex.approxJaccardScorer(id1, i1, j1, id2, i2, j2, approxTokenMatcher(_, _))
    val fuzzyJaccContains = tokenSimilarityIndex.approxJaccardContainScorer(id1, i1, j1, id2, i2, j2, approxTokenMatcher(_, _))
    val softJacc70 = tokenSimilarityIndex.approxJaccardScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.70))
    val softJacc85 = tokenSimilarityIndex.approxJaccardScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.85))
    val softJacc90 = tokenSimilarityIndex.approxJaccardScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.90))
    val softJacc95 = tokenSimilarityIndex.approxJaccardScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.95))
    val softJacc70Contains = tokenSimilarityIndex.approxJaccardContainScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.70))
    val softJacc85Contains = tokenSimilarityIndex.approxJaccardContainScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.85))
    val softJacc90Contains = tokenSimilarityIndex.approxJaccardContainScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.90))
    val softJacc95Contains = tokenSimilarityIndex.approxJaccardContainScorer(id1, i1, j1, id2, i2, j2, approxJaroWinklerScorer(_, _, 0.95))
    val jacc = tokenSimilarityIndex.jaccardScore(id1, i1, j1, id2, i2, j2)
    val jaccContains = tokenSimilarityIndex.containsJaccardScore(id1, i1, j1, id2, i2, j2)
    fv += alignFeatureIndexer.indexOf_?(BIAS_MATCH) -> 1.0
    for (sim <- SIM_BINS) {
      addFeatureToVector_?(fv, alignFeatureIndexer, char2Jacc, sim, CHAR2_JACCARD)
      addFeatureToVector_?(fv, alignFeatureIndexer, char2JaccContains, sim, CHAR2_JACCARD_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, char3Jacc, sim, CHAR3_JACCARD)
      addFeatureToVector_?(fv, alignFeatureIndexer, fuzzyJacc, sim, FUZZY_JACCARD)
      addFeatureToVector_?(fv, alignFeatureIndexer, fuzzyJaccContains, sim, FUZZY_JACCARD_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc70, sim, SOFT_JACCARD70)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc85, sim, SOFT_JACCARD85)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc90, sim, SOFT_JACCARD90)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc95, sim, SOFT_JACCARD95)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc70Contains, sim, SOFT_JACCARD70_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc85Contains, sim, SOFT_JACCARD85_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc90Contains, sim, SOFT_JACCARD90_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, softJacc95Contains, sim, SOFT_JACCARD95_CONTAINS)
      addFeatureToVector_?(fv, alignFeatureIndexer, jacc, sim, JACCARD)
      addFeatureToVector_?(fv, alignFeatureIndexer, jaccContains, sim, JACCARD_CONTAINS)
    }
    fv
  }

  def main(args: Array[String]) {
    val rawRecords = recordsColl.map(new Mention(_)).toArray
    val rawTexts = textsColl.map(new Mention(_)).toArray
    val rawMentions = rawRecords ++ rawTexts
    logger.info("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2fExample = new HashMap[String, FeatMentionExample]
    val id2fvecExample = new HashMap[String, FeatVecMentionExample]
    val id2cluster = FileHelper.getMapping1to2(args(0))
    val cluster2ids = getClusterToIds(id2cluster)
    for (m <- rawMentions) {
      id2mention(m.id) = m
      id2fExample(m.id) = toFeatExample(m)
      id2fvecExample(m.id) = toFeatVecExample(m)
      tokenSimilarityIndex.index(m.id, m.words)
      bigramSimilarityIndex.index(m.id, m.words)
      trigramSimilarityIndex.index(m.id, m.words)
    }

    val fExamples = id2fExample.values.toSeq
    val fvecExamples = id2fvecExample.values.toSeq

    // 1. Calculate candidate pairs using hotelname and localarea
    val blocker = getBlocker(cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))
    logger.info("#recordPairsMatched=" + getNumRecordPairsMatched(rawRecords, blocker))

    // 3. Segment HMM baseline
    var hmmParams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    hmmParams.setUniform_!
    hmmParams.normalize_!(1e-2)
    hmmParams = learnEMSegmentParamsHMM(20, rawMentions, hmmParams, 1e-2, 1e-2)
    decodeSegmentation("bft.hmm.true.txt", "bft.hmm.pred.txt", rawMentions, (m: Mention) => {
      val ex = id2fExample(m.id)
      val inferencer = new HMMSegmentationInferencer(labelIndexer, maxLengths, ex, hmmParams, hmmParams,
        InferSpec(0, 1, false, false, true, false, true, false, 1, 0))
      inferencer.bestWidget
    })

    // 4. WWT phase1 segment and learn from high-precision segmentations
    val hplAlignParams = newAlignParams(false, true, labelIndexer, alignFeatureIndexer)
    hplAlignParams.setUniform_!
    hplAlignParams.labelAligns(hotelNameIndex).increment_!(alignFeatureIndexer.indexOf_?(_gte(FUZZY_JACCARD, 0.6)), 1.0)
    hplAlignParams.labelAligns(localAreaIndex).increment_!(alignFeatureIndexer.indexOf_?(_gte(FUZZY_JACCARD, 0.6)), 1.0)
    hplAlignParams.labelAligns(starRatingIndex).increment_!(alignFeatureIndexer.indexOf_?(_gte(JACCARD, 1.0)), 1.0)

    val hplMentions = new ArrayBuffer[Mention]
    val hplOtherMentions = new ArrayBuffer[Seq[Mention]]
    for (m1 <- rawTexts) {
      for (m2 <- rawRecords if blocker.isPair(m1.id, m2.id)) {
        hplMentions += m1
        hplOtherMentions += Seq(m2)
      }
    }
    val hplLabeledMentions = new ArrayBuffer[Mention]
    hplLabeledMentions ++= rawMentions.filter(_.isRecord == true)
    hplLabeledMentions ++= decodeMatchOnlySegmentation(hplMentions, hplOtherMentions, id2cluster, hplAlignParams, 3.0)

    var crfParams = newSegmentParams(false, true, labelIndexer, featureIndexer)
    crfParams.setUniform_!
    crfParams = learnSupervisedSegmentParamsCRF(50, hplLabeledMentions, crfParams, 1, 1)
    // crfParams.output(logger.info(_))
    decodeSegmentation("bft.crf.true.txt", "bft.crf.pred.txt", rawMentions, (m: Mention) => {
      val ex = id2fvecExample(m.id)
      val inferencer = new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, crfParams, crfParams,
        InferSpec(0, 1, false, false, true, false, false, true, 1, 0))
      inferencer.bestWidget
    })

    // 5. Do alignment using gold-standard clusters

    /*
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
    */
  }
}
