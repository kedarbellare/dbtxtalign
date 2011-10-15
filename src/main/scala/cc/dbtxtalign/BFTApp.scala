package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.Imports._
import blocking.{AbstractBlocker, UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import mongo.KB
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.types.{Indexer, FtrVec}
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import collection.mutable.{HashSet, ArrayBuffer, HashMap}
import params.{ConstraintParams, Params}

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

  // Add translation dictionary
  val word_dt_index = wordIndexer.indexOf_!("dt")
  val word_downtown_indices = Seq("downtown").map(wordIndexer.indexOf_!(_))
  val word_mv_index = wordIndexer.indexOf_!("mv")
  val word_mission_valley_indices = Seq("mission", "valley").map(wordIndexer.indexOf_!(_))
  val word_ap_index = wordIndexer.indexOf_!("ap")
  val word_airport_pit_indices = Seq("airport", "pit").map(wordIndexer.indexOf_!(_))

  val localareaTranslationf = alignFeatureIndexer.indexOf_!("LOCAL_AREA_TRANSLATION")
  val highSimFuzzyJaccardf = alignFeatureIndexer.indexOf_!(_gte(FUZZY_JACCARD, 0.9))
  val lowSimFuzzyJaccardf = alignFeatureIndexer.indexOf_!(_lt(FUZZY_JACCARD, 0.2))
  val exactSimJaccardf = alignFeatureIndexer.indexOf_!(_gte(JACCARD, 1.0))
  val starPatternf = featureIndexer.indexOf_!("CONTAINS_STAR_PATTERN")

  override def getAlignFeatureVector(l: Int, id1: String, i1: Int, j1: Int,
                                     id2: String, i2: Int, j2: Int): FtrVec = {
    val fv = super.getAlignFeatureVector(l, id1, i1, j1, id2, i2, j2)
    if (l == localAreaIndex && j1 - i1 == 1 && j2 - i2 <= 2) {
      val windex1 = token2WordIndices(id1)(i1)
      if (windex1 == word_dt_index || windex1 == word_mv_index || windex1 == word_ap_index) {
        val windices2 = token2WordIndices(id2).slice(i2, j2).toSeq
        if ((windices2 == word_downtown_indices && windex1 == word_dt_index) ||
          (windices2 == word_mission_valley_indices && windex1 == word_mv_index) ||
          (windices2 == word_airport_pit_indices && windex1 == word_ap_index)) {
          // logger.info("translation found: " + windices2.map(wordIndexer(_)).mkString(" ") + " -> " + wordIndexer(windex1))
          fv += localareaTranslationf -> 1.0
        }
      }
    }
    fv
  }

  // Constraint features
  // 1. I(hotel >=_{fuzzy_jacc} 0.9) + I(local >=_{fuzzy_jacc} 0.9 || local is translation) - I(match) <= 1.0
  val highSimImpliesMatchf = constraintFeatureIndexer.indexOf_!("highSimilarityImpliesMatch")
  // 2. I(sim <_{fuzzy_jacc} 0.2)<= 0.1
  val lowSimAndMatchf = constraintFeatureIndexer.indexOf_!("lowSimilarityAndMatch")
  // 3. I(not(star) && starPattern) <= 0.0
  val starPatternMismatchf = constraintFeatureIndexer.indexOf_!("starRatingAndNotMatchesPattern")

  override def newConstraintMatchInferencer(ex: FeatVecAlignmentMentionExample, params: Params, counts: Params,
                                            constraintParams: ConstraintParams, constraintCounts: ConstraintParams,
                                            ispec: InferSpec, doUpdate: Boolean): CRFMatchSegmentationInferencer = {
    new CRFMatchSegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector, ex,
      params, counts, ispec, false, false) {
      override def scoreMatch(otherIndex: Int): Double = {
        super.scoreMatch(otherIndex) + {
          if (otherIndex < 0) 0.0
          else score(constraintParams.constraints, highSimImpliesMatchf)
        }
      }

      override def updateMatch(otherIndex: Int, v: Double) {
        if (doUpdate) super.updateMatch(otherIndex, v)
        if (otherIndex >= 0) update(constraintCounts.constraints, highSimImpliesMatchf, v)
      }

      override def scoreSingleEmission(a: Int, k: Int) = {
        var dotprod = super.scoreSingleEmission(a, k)
        if (featureVectorContains(featSeq(k), starPatternf) && a != starRatingIndex)
          dotprod -= score(constraintParams.constraints, starPatternMismatchf)
        dotprod
      }

      override def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
        if (!x.isNaN) {
          if (doUpdate) super.updateSingleEmissionCached(a, k, x)
          if (featureVectorContains(featSeq(k), starPatternf) && a != starRatingIndex)
            update(constraintCounts.constraints, starPatternMismatchf, -x)
        }
      }

      override def scoreSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int) = {
        val fv = alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj)
        var dotprod = score(alignParams(a), fv)
        if ((featureVectorContains(fv, highSimFuzzyJaccardf) && (a == hotelNameIndex || a == localAreaIndex)) ||
          (featureVectorContains(fv, localareaTranslationf) && a == localAreaIndex)) {
          dotprod -= score(constraintParams.constraints, highSimImpliesMatchf)
        }
        if (featureVectorContains(fv, lowSimFuzzyJaccardf)) {
          dotprod -= score(constraintParams.constraints, lowSimAndMatchf)
        }
        dotprod
      }

      override def updateSimilarity(otherIndex: Int, a: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double) {
        val fv = alignFeaturizer(a, ex.id, i, j, otherIds(otherIndex), oi, oj)
        if (doUpdate) update(alignCounts(a), fv, v)
        if ((featureVectorContains(fv, highSimFuzzyJaccardf) && (a == hotelNameIndex || a == localAreaIndex)) ||
          (featureVectorContains(fv, localareaTranslationf) && a == localAreaIndex)) {
          update(constraintCounts.constraints, highSimImpliesMatchf, -v)
        }
        if (featureVectorContains(fv, lowSimFuzzyJaccardf)) {
          update(constraintCounts.constraints, lowSimAndMatchf, -v)
        }
      }

      override def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double) {
        if (doUpdate) super.updateTransition(a, b, i, j, x)
      }

      override def updateStart(a: Int, j: Int, x: Double) {
        if (doUpdate) super.updateStart(a, j, x)
      }
    }
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
      token2WordIndices(m.id) = m.words.map(wordIndexer.indexOf_!(_))
      tokenSimilarityIndex.index(m.id, m.words)
      bigramSimilarityIndex.index(m.id, m.words)
      trigramSimilarityIndex.index(m.id, m.words)
    }

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
    hplAlignParams.labelAligns(hotelNameIndex).increment_!(highSimFuzzyJaccardf, 1.0)
    hplAlignParams.labelAligns(localAreaIndex).increment_!(highSimFuzzyJaccardf, 1.0)
    hplAlignParams.labelAligns(localAreaIndex).increment_!(localareaTranslationf, 1.0)
    hplAlignParams.labelAligns(starRatingIndex).increment_!(exactSimJaccardf, 1.0)

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
    val alignFvecExamples = new ArrayBuffer[FeatVecAlignmentMentionExample]
    var maxDegree = 0
    for (m1 <- rawMentions) {
      val clustOpt1 = id2cluster.get(m1.id)
      val ex = id2fvecExample(m1.id)
      var degree = 0
      val otherIds = new ArrayBuffer[String]
      val otherWordsSeq = new ArrayBuffer[Seq[String]]
      val otherSegmentations = new ArrayBuffer[Segmentation]
      val matchIds = new HashSet[String]
      for (m2 <- rawRecords if blocker.isPair(m1.id, m2.id)) {
        val clust2 = id2cluster(m2.id)
        val isMatch = clustOpt1.isDefined && clustOpt1.get == clust2
        val oex = id2fvecExample(m2.id)
        degree += 1
        if (isMatch) matchIds += m2.id
        otherIds += oex.id
        otherWordsSeq += oex.words
        otherSegmentations += oex.trueSegmentation
      }
      alignFvecExamples += new FeatVecAlignmentMentionExample(ex.id, ex.isRecord, ex.words, ex.possibleEnds,
        ex.featSeq, matchIds, ex.trueSegmentation, otherIds, otherWordsSeq, otherSegmentations)
      if (degree > maxDegree) maxDegree = degree
    }
    logger.info("#alignExamples=" + alignFvecExamples.size + " maxDegree=" + maxDegree)

    var params = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    params.setUniform_!
    params = learnSupervisedAlignParamsCRF(10, alignFvecExamples, params, 1, 1)
    params.output(logger.info(_))

    // 6. Constraint-based learning
    val numAlignExamples = alignFvecExamples.size
    var constraintParams = newConstraintParams(false, true, constraintFeatureIndexer)
    val defaultConstraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)
    constraintParams.setUniform_!
    defaultConstraintCounts.setUniform_!
    defaultConstraintCounts.constraints.increment_!(highSimImpliesMatchf, numAlignExamples * 1.0)
    defaultConstraintCounts.constraints.increment_!(lowSimAndMatchf, numAlignExamples * 0.1)
    defaultConstraintCounts.constraints.increment_!(starPatternMismatchf, numAlignExamples * 0.01)
    params.setUniform_!
    for (iter <- 1 to 10) {
      // optimize constraint params first
      constraintParams = learnSemiSupervisedConstraintParamsCRF(20, alignFvecExamples, params, constraintParams,
        defaultConstraintCounts, 0.01)
      constraintParams.output(logger.info(_))
      params = learnSemiSupervisedAlignParamsCRF(10, alignFvecExamples, params, constraintParams, 1)
    }
    params.output(logger.info(_))
  }
}
