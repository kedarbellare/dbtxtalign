package cc.dbtxtalign

import blocking.AbstractBlocker
import collection.mutable.{ArrayBuffer, HashMap}
import java.io.PrintWriter
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.types.{ParamUtils, FtrVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.dynprog.{ProbStats, InferSpec}
import optimization.linesearch.ArmijoLineSearchMinimization
import optimization.gradientBasedMethods.stats.OptimizerStats
import optimization.gradientBasedMethods.{Objective, Optimizer, LBFGS}
import params._
import optimization.stopCriteria.{AverageValueDifference, GradientL2Norm}
import org.riedelcastro.nurupo.HasLogger

/**
 * @author kedar
 */


trait AbstractAlign extends HasLogger {
  val wordIndexer = new Indexer[String]
  val labelIndexer = new Indexer[String]
  val wordFeatureIndexer = new Indexer[String]
  val featureIndexer = new Indexer[String]
  val alignFeatureIndexer = new Indexer[String]
  val maxLengths = new ArrayBuffer[Int]
  val otherLabelIndex = labelIndexer.indexOf_!("O")

  def simplify(s: String): String

  def L = labelIndexer.size

  def W = wordIndexer.size

  def WF = wordFeatureIndexer.size

  def F = featureIndexer.size

  def AF = alignFeatureIndexer.size

  def _gte(simStr: String, threshold: Double) = simStr + ">=" + threshold

  def _lt(simStr: String, threshold: Double) = simStr + "<" + threshold

  def addThresholdFeaturesToIndexer(indexer: Indexer[String], simStr: String, threshold: Double) {
    indexer += _gte(simStr, threshold)
    indexer += _lt(simStr, threshold)
  }

  def addFeatureToVector_?(fv: FtrVec, indexer: Indexer[String], score: Double, threshold: Double,
                           simStr: String) = {
    if (score >= threshold) {
      val trueIdx = indexer.indexOf_?(_gte(simStr, threshold))
      if (trueIdx >= 0) fv += trueIdx -> 1.0
    } else {
      val falseIdx = indexer.indexOf_?(_lt(simStr, threshold))
      if (falseIdx >= 0) fv += falseIdx -> 1.0
    }
  }

  def adjustSegmentation(words: Seq[String], segmentation: Segmentation): Segmentation = {
    val adjSegmentation = new Segmentation(segmentation.length)
    val puncPattern = "^[^A-Za-z0-9]*$"
    forIndex(segmentation.numSegments, {
      s: Int =>
        val segment = segmentation.segment(s)
        val mod_begin = {
          var i = segment.begin
          while (i < segment.end && words(i).matches(puncPattern)) i += 1
          i
        }
        if (mod_begin == segment.end) {
          // whole phrase is punctuations
          require(adjSegmentation.append(Segment(segment.begin, segment.end, otherLabelIndex)))
        } else {
          if (mod_begin > segment.begin) {
            // prefix is punctuation
            require(adjSegmentation.append(Segment(segment.begin, mod_begin, otherLabelIndex)))
          }
          val mod_end = {
            var i = segment.end - 1
            while (i >= mod_begin && words(i).matches(puncPattern)) i -= 1
            i + 1
          }
          if (mod_end == segment.end) {
            // rest is valid
            require(adjSegmentation.append(Segment(mod_begin, segment.end, segment.label)))
          } else {
            // suffix is punctuation
            require(adjSegmentation.append(Segment(mod_begin, mod_end, segment.label)))
            require(adjSegmentation.append(Segment(mod_end, segment.end, otherLabelIndex)))
          }
        }
    })
    adjSegmentation
  }

  def getBlocker(cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker

  def getFeatureSequence(m: Mention, featurizer: (String) => Int): Seq[Int] = {
    m.words.map(featurizer(_))
  }

  def getFeatureSequence_!(m: Mention, indexer: Indexer[String], featurizer: (String) => String): Seq[Int] = {
    getFeatureSequence(m, {
      s: String => indexer.indexOf_!(featurizer(s))
    })
  }

  def getFeatureVectorSequence(m: Mention, featurizer: (Seq[String], Int) => Seq[Int]): Seq[FtrVec] = {
    for (i <- 0 until m.words.length) yield {
      val fv = new FtrVec
      featurizer(m.words, i).filter(_ >= 0).toSet.foreach({
        f: Int => fv += f -> 1.0
      })
      fv
    }
  }

  def getFeatureVectorSequence_!(m: Mention, indexer: Indexer[String],
                                 featurizer: (Seq[String], Int) => Seq[String]): Seq[FtrVec] = {
    getFeatureVectorSequence(m, {
      (ws: Seq[String], i: Int) => featurizer(ws, i).map(indexer.indexOf_!(_))
    })
  }

  def getFeatureVectorSequence_?(m: Mention, indexer: Indexer[String],
                                 featurizer: (Seq[String], Int) => Seq[String]): Seq[FtrVec] = {
    getFeatureVectorSequence(m, {
      (ws: Seq[String], i: Int) => featurizer(ws, i).map(indexer.indexOf_?(_))
    })
  }

  def getAlignFeatureVector(l: Int, phrase: Seq[String], otherPhrase: Seq[String]): FtrVec = new FtrVec

  def getSegmentation_!(m: Mention, indexer: Indexer[String]): Segmentation = {
    Segmentation.fromBIO(m.trueBioLabels, indexer.indexOf_!(_))
  }

  def getSegmentationAndMaxLength_!(m: Mention, indexer: Indexer[String], maxLengths: ArrayBuffer[Int]): Segmentation = {
    val useOracle = true
    val trueSeg = getSegmentation_!(m, indexer)
    // default length is 1
    while (indexer.size != maxLengths.size) maxLengths += 1
    // accumulate max lengths overall
    // TODO (cheating oracle!)
    if (useOracle || m.isRecord)
      trueSeg.segments.filter(_.label != otherLabelIndex).foreach(s => {
        maxLengths(s.label) = math.max(s.end - s.begin, maxLengths(s.label))
      })
    trueSeg
  }

  def toFeatVecExample(m: Mention): FeatVecMentionExample

  def toFeatExample(m: Mention): FeatMentionExample

  def getClusterToIds(id2cluster: HashMap[String, String]): HashMap[String, Seq[String]] = {
    val cluster2ids = new HashMap[String, Seq[String]]
    for ((id, cluster) <- id2cluster)
      cluster2ids(cluster) = cluster2ids.getOrElse(cluster, Seq.empty[String]) ++ Seq(id)
    cluster2ids
  }

  def getMaxRecordsMatched(rawTexts: Seq[Mention], rawRecords: Seq[Mention],
                           blocker: AbstractBlocker): Int = {
    var maxRecordsMatched = 0
    for (m <- rawTexts) {
      var numRecords = 0
      for (r <- rawRecords if blocker.isPair(m.id, r.id)) numRecords += 1
      if (maxRecordsMatched < numRecords) maxRecordsMatched = numRecords
    }
    maxRecordsMatched
  }

  def getNumRecordPairsMatched(rawRecords: Seq[Mention], blocker: AbstractBlocker): Int = {
    var numRecordPairs = 0
    for (ex1 <- rawRecords) {
      val id1 = ex1.id
      for (ex2 <- rawRecords) {
        val id2 = ex2.id
        if (id1 != id2 && blocker.isPair(id1, id2)) {
          numRecordPairs += 1
        }
      }
    }
    numRecordPairs
  }

  def newSegmentParams(isProb: Boolean, isDense: Boolean,
                       labelIndexer: Indexer[String], featureIndexer: Indexer[String]): SegmentParams = {
    import ParamUtils._
    val L = labelIndexer.size
    val F = featureIndexer.size
    val starts = if (isProb) newPrVec(isDense, L) else newWtVec(isDense, L)
    val transitions = if (isProb) newPrVecArray(isDense, L, L) else newWtVecArray(isDense, L, L)
    val emissions = if (isProb) newPrVecArray(isDense, L, F) else newWtVecArray(isDense, L, F)
    new SegmentParams(
      new TransitionParams(labelIndexer, starts, transitions),
      new EmissionParams(labelIndexer, featureIndexer, emissions))
  }

  def newAlignParams(isProb: Boolean, isDense: Boolean,
                      labelIndexer: Indexer[String], alignFeatureIndexer: Indexer[String]): AlignParams = {
    import ParamUtils._
    val L = labelIndexer.size
    val AF = alignFeatureIndexer.size
    val labelAligns = if (isProb) newPrVecArray(isDense, L, AF) else newWtVecArray(isDense, L, AF)
    new AlignParams(labelIndexer, alignFeatureIndexer, labelAligns)
  }

  def newParams(isProb: Boolean, isDense: Boolean,
                labelIndexer: Indexer[String], featureIndexer: Indexer[String],
                alignFeatureIndexer: Indexer[String]): Params = {
    import ParamUtils._
    val L = labelIndexer.size
    val F = featureIndexer.size
    val AF = alignFeatureIndexer.size
    val starts = if (isProb) newPrVec(isDense, L) else newWtVec(isDense, L)
    val transitions = if (isProb) newPrVecArray(isDense, L, L) else newWtVecArray(isDense, L, L)
    val emissions = if (isProb) newPrVecArray(isDense, L, F) else newWtVecArray(isDense, L, F)
    val aligns = if (isProb) newPrVecArray(isDense, L, AF) else newWtVecArray(isDense, L, AF)
    new Params(
      new TransitionParams(labelIndexer, starts, transitions),
      new EmissionParams(labelIndexer, featureIndexer, emissions),
      new AlignParams(labelIndexer, alignFeatureIndexer, aligns))
  }

  // Learning functions
  // 1. Segment HMM trained with EM
  def learnEMSegmentParamsHMM(numIter: Int, examples: Seq[FeatMentionExample], hmmParams: SegmentParams,
                              unlabeledWeight: Double, smoothing: Double): SegmentParams = {
    var segparams = hmmParams
    for (iter <- 1 to numIter) {
      val segcounts = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
      var loglike = 0.0
      var exnum = 0
      for (ex <- examples) {
        val stepSize = if (ex.isRecord) 1 else unlabeledWeight
        val inferencer = new HMMSegmentationInferencer(labelIndexer, maxLengths, ex, segparams, segcounts,
          InferSpec(iter, 1, false, ex.isRecord, false, false, true, false, 1, stepSize))
        loglike += inferencer.logZ
        inferencer.updateCounts
        exnum += 1
        if (exnum % 1000 == 0) logger.info("Processed " + exnum + "/" + examples.size)
      }
      logger.info("*** iteration[" + iter + "] loglike=" + loglike)
      segcounts.normalize_!(smoothing)
      segparams = segcounts
    }
    segparams
  }

  def decodeSegmentation(trueFilename: String, predFilename: String, mentions: Seq[Mention],
                          decoder: (Mention) => Segmentation) {
    val trueOut = new PrintWriter(trueFilename)
    val predOut = new PrintWriter(predFilename)
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    for (m <- mentions if !m.isRecord) {
      val predWidget = decoder(m)
      val predSeg = adjustSegmentation(m.words, predWidget)
      val trueSeg = adjustSegmentation(m.words, getSegmentation_!(m, labelIndexer))
      tokenPerf.add(trueSeg, predSeg)
      segmentPerf.add(trueSeg, predSeg)
      perLabelPerf.add(trueSeg, predSeg)
      predOut.println(SegmentationHelper.toFullString(m.words, predSeg, labelIndexer(_)))
      trueOut.println(SegmentationHelper.toFullString(m.words, trueSeg, labelIndexer(_)))
    }
    tokenPerf.output(logger.info(_))
    perLabelPerf.output(logger.info(_))
    segmentPerf.output(logger.info(_))
    trueOut.close()
    predOut.close()
  }

  // 2. Segment CRF trained on high-precision segmentations
  def learnSupervisedSegmentParamsCRF(numIter: Int, examples: Seq[FeatVecMentionExample], params: SegmentParams,
                                      textWeight: Double, invVariance: Double): SegmentParams = {
    // calculate constraints once
    val constraints = newSegmentParams(false, true, labelIndexer, featureIndexer)
    for (ex <- examples) {
      val stepSize = if (ex.isRecord) 1.0 else textWeight
      new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, params, constraints,
        InferSpec(0, 1, false, true, false, false, false, true, 1, stepSize)).updateCounts
    }
    // constraints.output(logger.info(_))

    val objective = new ACRFObjective[SegmentParams](params, invVariance) {
      def logger = Logger.getLogger("high-precision-crf-trainer")

      def getValueAndGradient = {
        val expectations = newSegmentParams(false, true, labelIndexer, featureIndexer)
        val stats = new ProbStats()
        for (ex <- examples) {
          val stepSize = if (ex.isRecord) 1.0 else textWeight
          // constraints cost
          val truthInfer = new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, params, constraints,
            InferSpec(0, 1, false, true, false, false, false, true, 1, 0))
          // expectations
          val predInfer = new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, params, expectations,
            InferSpec(0, 1, false, false, false, false, false, true, 1, -stepSize))
          predInfer.updateCounts
          stats += (truthInfer.stats - predInfer.stats) * stepSize
        }
        expectations.add_!(constraints, 1)
        (expectations, stats)
      }
    }

    // optimize
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-3)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished segmentation learning only epoch=" + (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)

    params
  }
/*
  def getHighPrecisionLabeledExamples(fvecExamples: Seq[FeatVecMentionExample],
                                      blocker: AbstractBlocker,
                                      approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                                      approxMatchThresholds: Seq[Double], approxSumMatchThreshold: Double,
                                      id2cluster: HashMap[String, String]): Seq[FeatVecMentionExample] = {
    val textExamples = (for (ex <- fvecExamples if !ex.isRecord) yield ex).toSeq
    val recordExamples = (for (ex <- fvecExamples if ex.isRecord) yield ex).toSeq
    var crfParams = newSegmentParams(false, true, labelIndexer, featureIndexer)
    crfParams.setUniform_!

    logger.info("=====================================================================================")
    var numFoundMatch = 0
    val hplExamples = new ArrayBuffer[FeatVecMentionExample]
    hplExamples ++= recordExamples
    for (ex1 <- textExamples) {
      val clust1 = id2cluster.getOrElse(ex1.id, "NULL")
      val matchedExamples = new ArrayBuffer[FeatVecMentionExample]
      for (ex2 <- recordExamples if blocker.isPair(ex1.id, ex2.id)) {
        val ex = new FeatVecMatchOnlyMentionExample(ex1.id, ex1.isRecord, ex1.words, ex1.possibleEnds, ex1.featSeq,
          ex1.trueSegmentation, ex2.id, ex2.words, ex2.trueSegmentation)
        val crfSegMatch = new CRFSegmentAndMatchWWT(labelIndexer, maxLengths, approxMatchers, approxMatchThresholds,
          ex, crfParams, crfParams, InferSpec(0, 1, false, false, true, false, true, false, 1, 0))
        if (crfSegMatch.logVZ >= approxSumMatchThreshold) {
          logger.info("")
          logger.info("matchScore: " + crfSegMatch.logVZ + " clusterMatch: " + (clust1 == id2cluster(ex2.id)))
          logger.info("recordWords: " + ex2.words.mkString(" "))
          logger.info("recordSegmentation: " + ex2.trueSegmentation)
          logger.info("words: " + ex1.words.mkString(" "))
          logger.info("matchSegmentation: " + crfSegMatch.bestWidget)
          logger.info("segmentation: " + ex1.trueSegmentation)
          matchedExamples += new FeatVecMentionExample(ex1.id, ex1.isRecord, ex1.words, ex1.possibleEnds,
            ex1.featSeq, crfSegMatch.bestWidget)
        }
      }

      if (matchedExamples.size == 1) {
        logger.info("=====================================================================================")
        hplExamples += matchedExamples(0)
        numFoundMatch += 1
      }
    }
    logger.info(numFoundMatch + "/" + textExamples.length)

    hplExamples.toSeq
  }

  def getHighPrecisionLabeledExamplesHMM(fvecExamples: Seq[FeatMentionExample],
                                         blocker: AbstractBlocker,
                                         approxMatchers: Seq[(Seq[String], Seq[String]) => Double],
                                         approxMatchThresholds: Seq[Double], approxSumMatchThreshold: Double,
                                         id2cluster: HashMap[String, String]): Seq[FeatMentionExample] = {
    val textExamples = (for (ex <- fvecExamples if !ex.isRecord) yield ex).toSeq
    val recordExamples = (for (ex <- fvecExamples if ex.isRecord) yield ex).toSeq
    var hmmParams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    hmmParams.setUniform_!

    logger.info("=====================================================================================")
    var numFoundMatch = 0
    val hplExamples = new ArrayBuffer[FeatMentionExample]
    hplExamples ++= recordExamples
    for (ex1 <- textExamples) {
      val clust1 = id2cluster.getOrElse(ex1.id, "NULL")
      val matchedExamples = new ArrayBuffer[FeatMentionExample]
      for (ex2 <- recordExamples if blocker.isPair(ex1.id, ex2.id)) {
        val ex = new FeatMatchOnlyMentionExample(ex1.id, ex1.isRecord, ex1.words, ex1.possibleEnds, ex1.featSeq,
          ex1.trueSegmentation, ex2.id, ex2.words, ex2.trueSegmentation)
        val hmmSegMatch = new HMMSegmentAndMatchWWT(labelIndexer, maxLengths, approxMatchers, approxMatchThresholds,
          ex, hmmParams, hmmParams, InferSpec(0, 1, false, false, true, false, true, false, 1, 0))
        if (hmmSegMatch.logVZ >= approxSumMatchThreshold) {
          logger.info("")
          logger.info("matchScore: " + hmmSegMatch.logVZ + " clusterMatch: " + (clust1 == id2cluster(ex2.id)))
          logger.info("recordWords: " + ex2.words.mkString(" "))
          logger.info("recordSegmentation: " + ex2.trueSegmentation)
          logger.info("words: " + ex1.words.mkString(" "))
          logger.info("matchSegmentation: " + hmmSegMatch.bestWidget)
          logger.info("segmentation: " + ex1.trueSegmentation)
          matchedExamples += new FeatMentionExample(ex1.id, ex1.isRecord, ex1.words, ex1.possibleEnds,
            ex1.featSeq, hmmSegMatch.bestWidget)
        }
      }

      if (matchedExamples.size == 1) {
        logger.info("=====================================================================================")
        hplExamples += matchedExamples(0)
        numFoundMatch += 1
      }
    }
    logger.info(numFoundMatch + "/" + textExamples.length)

    hplExamples.toSeq
  }

  // 3. Supervised alignment learning
  def learnSupervisedAlignParamsCRF(numIter: Int, examples: Seq[FeatVecAlignmentMentionExample], params: Params,
                                    alignFeaturizer: (Int, Seq[String], Seq[String]) => FtrVec,
                                    textWeight: Double, invVariance: Double): Params = {
    // calculate constraints once
    val constraints = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    var numEx = 0
    for (ex <- examples) {
      val stepSize = if (ex.isRecord) 1.0 else textWeight
      new CRFMatchSegmentationInferencer(labelIndexer, alignFeatureIndexer, maxLengths, ex, params, constraints,
        InferSpec(0, 1, false, false, false, false, false, true, 1, stepSize),
        true, true, alignFeaturizer).updateCounts
      numEx += 1
      if (numEx % 1000 == 0) logger.info("Processed [constraints] " + numEx + "/" + examples.size + " examples ...")
    }
    // constraints.output(logger.info(_))

    val objective = new ACRFObjective[Params](params, invVariance) {
      def logger = Logger.getLogger("supervised-crf-align-trainer")

      def getValueAndGradient = {
        val expectations = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
        val stats = new ProbStats()
        var numEx = 0
        for (ex <- examples) {
          val stepSize = if (ex.isRecord) 1.0 else textWeight
          // constraints cost
          val truthInfer = new CRFMatchSegmentationInferencer(labelIndexer, alignFeatureIndexer, maxLengths,
            ex, params, constraints, InferSpec(0, 1, false, false, false, false, false, true, 1, 0),
            true, true, alignFeaturizer)
          // expectations
          val predInfer = new CRFMatchSegmentationInferencer(labelIndexer, alignFeatureIndexer, maxLengths,
            ex, params, expectations, InferSpec(0, 1, false, false, false, false, false, true, 1, -stepSize),
            false, false, alignFeaturizer)
          predInfer.updateCounts
          stats += (truthInfer.stats - predInfer.stats) * stepSize
          numEx += 1
          if (numEx % 1000 == 0) logger.info("Processed [expectations] " + numEx + "/" + examples.size + " examples ...")
        }
        expectations.add_!(constraints, 1)
        (expectations, stats)
      }
    }

    // optimize
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-3)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished alignment+segmentation learning only epoch=" + (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)

    params
  }
  */
}