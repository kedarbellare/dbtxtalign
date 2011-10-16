package cc.dbtxtalign

import blocking.AbstractBlocker
import collection.mutable.{HashSet, ArrayBuffer, HashMap}
import fastsim.SimilarityIndex
import java.io.PrintWriter
import org.apache.log4j.Logger
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment._
import cc.refectorie.user.kedarb.dynprog.{ProbStats, InferSpec}
import optimization.gradientBasedMethods.stats.OptimizerStats
import params._
import optimization.stopCriteria.AverageValueDifference
import org.riedelcastro.nurupo.HasLogger
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.duration._
import phrasematch.ObjectStringScorer
import cc.refectorie.user.kedarb.dynprog.types.{ParamUtils, FtrVec, Indexer}
import optimization.projections.BoundsProjection
import optimization.linesearch.{InterpolationPickFirstStep, ArmijoLineSearchMinimizationAlongProjectionArc, ArmijoLineSearchMinimization}
import optimization.gradientBasedMethods.{ProjectedGradientDescent, Objective, Optimizer, LBFGS}
import java.util.concurrent.CountDownLatch

/**
 * @author kedar
 */


trait AbstractAlign extends HasLogger {
  val wordIndexer = new Indexer[String]
  val labelIndexer = new Indexer[String]
  val wordFeatureIndexer = new Indexer[String]
  val featureIndexer = new Indexer[String]
  val alignFeatureIndexer = new Indexer[String]
  val constraintFeatureIndexer = new Indexer[String]

  val maxLengths = new ArrayBuffer[Int]

  val otherLabelIndex = labelIndexer.indexOf_!("O")

  val positiveProjection = new BoundsProjection(0, Double.PositiveInfinity)

  val token2WordIndices = new HashMap[String, Seq[Int]]
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

  val MAXTIME: Long = (30 days).toMillis

  def simplify(s: String): String

  def L = labelIndexer.size

  def W = wordIndexer.size

  def WF = wordFeatureIndexer.size

  def F = featureIndexer.size

  def AF = alignFeatureIndexer.size

  def CF = constraintFeatureIndexer.size

  def numWorkers = DBAlignConfig.get[Int]("numThreads", 2)

  def batchSize = DBAlignConfig.get[Int]("batchSize", 10)

  def numBatches(size: Int) = (size + batchSize - 1) / batchSize

  def _gte(simStr: String, threshold: Double) = simStr + ">=" + threshold

  def _lt(simStr: String, threshold: Double) = simStr + "<" + threshold

  def addThresholdFeaturesToIndexer(indexer: Indexer[String], simStr: String, threshold: Double) {
    indexer += _gte(simStr, threshold)
    indexer += _lt(simStr, threshold)
  }

  def addGTEFeatureToVector_?(fv: FtrVec, indexer: Indexer[String], score: Double, threshold: Double,
                                simStr: String) = {
    if (score >= threshold) {
      val trueIdx = indexer.indexOf_?(_gte(simStr, threshold))
      if (trueIdx >= 0) fv += trueIdx -> 1.0
    }
  }

  def addLTFeatureToVector_?(fv: FtrVec, indexer: Indexer[String], score: Double, threshold: Double,
                                simStr: String) = {
    if (score < threshold) {
      val falseIdx = indexer.indexOf_?(_lt(simStr, threshold))
      if (falseIdx >= 0) fv += falseIdx -> 1.0
    }
  }

  def addFeatureToVector_?(fv: FtrVec, indexer: Indexer[String], score: Double, threshold: Double,
                           simStr: String) = {
    addGTEFeatureToVector_?(fv, indexer, score, threshold, simStr)
    addLTFeatureToVector_?(fv, indexer, score, threshold, simStr)
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

  def featureVectorContains(fv: FtrVec, index: Int): Boolean =
    fv.exists(keyval => keyval._1 == index && keyval._2 != 0.0)

  def getAlignFeatureVector(l: Int, id1: String, i1: Int, j1: Int, id2: String, i2: Int, j2: Int): FtrVec = {
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

  def getBIOFromSegmentation(segmentation: Segmentation): Seq[String] = {
    val OTHER = labelIndexer(otherLabelIndex)
    val bioLabels = Array.fill[String](segmentation.length)(OTHER)
    forIndex(segmentation.numSegments, (i: Int) => {
      val segment = segmentation.segment(i)
      if (segment.label != otherLabelIndex) {
        val LBL = labelIndexer(segment.label)
        forIndex(segment.begin, segment.end, (k: Int) => {
          bioLabels(k) = ("I-" + LBL)
        })
        bioLabels(segment.begin) = ("B-" + LBL)
      }
    })
    bioLabels.toSeq
  }

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

  def toFeatVecAlignExample(m: Mention, oms: Seq[Mention], id2cluster: HashMap[String, String]) = {
    val mclust = id2cluster.get(m.id)
    val mex = toFeatVecExample(m)

    val trueMatchIds = new HashSet[String]
    if (mclust.isDefined)
      oms.foreach(om => {
        val omclust = id2cluster.get(om.id)
        if (omclust.isDefined && omclust.get == mclust.get)
          trueMatchIds += om.id
      })

    new FeatVecAlignmentMentionExample(m.id, m.isRecord, m.words, mex.possibleEnds,
      mex.featSeq, trueMatchIds, mex.trueSegmentation, oms.map(_.id), oms.map(_.words),
      oms.map(getSegmentation_!(_, labelIndexer)))
  }

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

  def newConstraintParams(isProb: Boolean, isDense: Boolean,
                          constraintFeatureIndexer: Indexer[String]): ConstraintParams = {
    import ParamUtils._
    val CF = constraintFeatureIndexer.size
    val constraints = if (isProb) newPrVec(isDense, CF) else newWtVec(isDense, CF)
    new ConstraintParams(constraintFeatureIndexer, constraints)
  }

  // Learning functions
  sealed trait LearnMessage

  case object MasterStartMessage extends LearnMessage

  case object MasterDoneMessage extends LearnMessage

  case class ProcessMentions(ms: Seq[Mention]) extends LearnMessage

  case class ProcessAlignMentions(ms: Seq[Mention], aligns: Seq[Seq[Mention]]) extends LearnMessage

  case object FinishedMentions extends LearnMessage

  case class SegmentResult(stats: ProbStats, counts: SegmentParams) extends LearnMessage

  case class AlignResult(stats: ProbStats, counts: AlignParams) extends LearnMessage

  case class AlignPerfResult(total: Int, tp: Int, fp: Int, fn: Int, labeledMentions: Seq[Mention]) extends LearnMessage

  trait SegmentationWorker extends Actor {
    val stats = new ProbStats
    var progress = 0

    def iter: Int

    def params: SegmentParams

    def counts: SegmentParams

    def unlabeledWeight: Double

    def latch: CountDownLatch

    def updateProgress(done: Boolean = false) {
      progress += 1
      if (progress % 1000 == 0 || done) logger.info("Processed " + progress + " mentions")
    }

    def doInfer(m: Mention, stepSize: Double)

    def receive = {
      case ProcessMentions(ms) =>
        // perform expectation step on each mention
        ms.foreach(m => {
          val stepSize = if (m.isRecord) 1.0 else unlabeledWeight
          doInfer(m, stepSize)
          updateProgress()
        })
      case FinishedMentions =>
        updateProgress(true)
        self.channel ! SegmentResult(stats, counts)
        self.stop()
    }

    override def postStop() {
      super.postStop()
      latch.countDown()
    }
  }

  // 1. Segment HMM trained with EM
  class HMMWorker(val iter: Int, val params: SegmentParams, val unlabeledWeight: Double,
                  val latch: CountDownLatch) extends SegmentationWorker {
    val counts = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)

    def doInfer(m: Mention, stepSize: Double) {
      val inferencer = new HMMSegmentationInferencer(labelIndexer, maxLengths, toFeatExample(m), params, counts,
        InferSpec(iter, 1, false, m.isRecord, false, false, true, false, 1, stepSize))
      inferencer.updateCounts
      stats += inferencer.stats
    }
  }

  def learnEMSegmentParamsHMM(numIter: Int, mentions: Seq[Mention], hmmParams: SegmentParams,
                              unlabeledWeight: Double, smoothing: Double): SegmentParams = {
    var params = hmmParams
    for (iter <- 1 to numIter) {
      // create the workers
      val latch = new CountDownLatch(numWorkers)
      val workers = Vector.fill(numWorkers)(actorOf(new HMMWorker(iter, params, unlabeledWeight, latch)).start())
      for (b <- 0 until numBatches(mentions.size)) {
        workers(b % numWorkers) ! ProcessMentions(mentions.slice(b * batchSize, (b + 1) * batchSize))
      }

      val counts = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
      var loglike = 0.0
      workers.foreach(worker => {
        for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentResult]) {
          // get work from each worker
          loglike += result.stats.logZ
          counts.add_!(result.counts, 1)
        }
      })
      latch.await()

      logger.info("*** iteration[" + iter + "] loglike=" + loglike)
      counts.normalize_!(smoothing)
      params = counts
    }
    params
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
  class CRFConstraintsWorker(val iter: Int, val params: SegmentParams,
                             val unlabeledWeight: Double, val latch: CountDownLatch)
    extends SegmentationWorker {
    val counts = newSegmentParams(false, true, labelIndexer, featureIndexer)

    def doInfer(m: Mention, stepSize: Double) {
      val inferencer = new CRFSegmentationInferencer(labelIndexer, maxLengths, toFeatVecExample(m), params, counts,
        InferSpec(iter, 1, false, true, false, false, false, true, 1, stepSize))
      inferencer.updateCounts
      stats += inferencer.stats
    }
  }

  class CRFExpectationWorker(val iter: Int, val params: SegmentParams,
                             val unlabeledWeight: Double, val latch: CountDownLatch)
    extends SegmentationWorker {
    val counts = newSegmentParams(false, true, labelIndexer, featureIndexer)

    def doInfer(m: Mention, stepSize: Double) {
      val ex = toFeatVecExample(m)
      val truthInferencer = new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, params, counts,
        InferSpec(0, 1, false, true, false, false, false, true, 1, 0))
      // expectations
      val predInferencer = new CRFSegmentationInferencer(labelIndexer, maxLengths, ex, params, counts,
        InferSpec(0, 1, false, false, false, false, false, true, 1, -stepSize))
      predInferencer.updateCounts
      stats += (truthInferencer.stats - predInferencer.stats) * stepSize
    }
  }

  def learnSupervisedSegmentParamsCRF(numIter: Int, mentions: Seq[Mention], params: SegmentParams,
                                      unlabeledWeight: Double, invVariance: Double): SegmentParams = {
    // create the workers
    val constraintLatch = new CountDownLatch(numWorkers)
    val constraintWorkers = Vector.fill(numWorkers)(
      actorOf(new CRFConstraintsWorker(0, params, unlabeledWeight, constraintLatch)).start())
    for (b <- 0 until numBatches(mentions.size)) {
      constraintWorkers(b % numWorkers) ! ProcessMentions(mentions.slice(b * batchSize, (b + 1) * batchSize))
    }

    val constraints = newSegmentParams(false, true, labelIndexer, featureIndexer)
    constraintWorkers.foreach(worker => {
      for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentResult]) {
        // get work from each worker
        constraints.add_!(result.counts, 1)
      }
    })
    constraintLatch.await()

    val objective = new ACRFObjective[SegmentParams](params, invVariance) {
      val logger = Logger.getLogger("high-precision-crf-trainer")

      def getValueAndGradient = {
        val expectationLatch = new CountDownLatch(numWorkers)
        val workers = Vector.fill(numWorkers)(
          actorOf(new CRFExpectationWorker(0, params, unlabeledWeight, expectationLatch)).start())
        for (b <- 0 until numBatches(mentions.size)) {
          workers(b % numWorkers) ! ProcessMentions(mentions.slice(b * batchSize, (b + 1) * batchSize))
        }

        val expectations = newSegmentParams(false, true, labelIndexer, featureIndexer)
        val stats = new ProbStats()
        workers.foreach(worker => {
          for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentResult]) {
            // get work from each worker
            stats += result.stats
            expectations.add_!(result.counts, 1)
          }
        })
        expectationLatch.await()
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

  // 3. High-precision alignment segmentation
  class ViterbiAlignmentMatchOnlyWorker(val id2cluster: HashMap[String, String], val alignParams: AlignParams,
                                        val matchThreshold: Double, val latch: CountDownLatch)
    extends Actor {
    var numMatches = 0
    var tpMatches = 0
    var fpMatches = 0
    var fnMatches = 0
    val hplMentions = new ArrayBuffer[Mention]

    def doAlignInfer(m: Mention, oms: Seq[Mention]) {
      val ex = toFeatVecAlignExample(m, oms, id2cluster)
      if (ex.trueMatchIds.size > 0) numMatches += 1

      val inferencer = new CRFMatchOnlySegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector,
        ex, alignParams, alignParams, InferSpec(0, 1, false, false, true, false, false, true, 1, 0),
        false, false)
      val predMatchIds = inferencer.bestWidget.matchIds
      val isMatch = predMatchIds.size > 0 && ex.trueMatchIds(predMatchIds.head)
      if (inferencer.logVZ >= matchThreshold) {
        if (isMatch) tpMatches += 1 else fpMatches += 1
        logger.info("")
        logger.info("id[" + ex.id + "]")
        logger.info("matchScore: " + inferencer.logVZ + " clusterMatch: " + isMatch)
        logger.info("recordWords: " + ex.otherWordsSeq.head.mkString(" "))
        logger.info("recordSegmentation: " + getBIOFromSegmentation(ex.otherSegmentations.head).mkString(" "))
        logger.info("words: " + ex.words.mkString(" "))

        // add high-precision labeled mention to list
        val predBIOLabels = getBIOFromSegmentation(inferencer.bestWidget.segmentation)
        logger.info("matchSegmentation: " + predBIOLabels.mkString(" "))
        logger.info("truthSegmentation: " + getBIOFromSegmentation(ex.trueSegmentation).mkString(" "))
        hplMentions += new Mention(m.id, m.isRecord, m.words, predBIOLabels)
      } else if (isMatch) fnMatches += 1
    }

    def receive = {
      case ProcessAlignMentions(ms, aligns) =>
        forIndex(ms.length, (i: Int) => {
          doAlignInfer(ms(i), aligns(i))
        })
      case FinishedMentions =>
        self.channel ! AlignPerfResult(numMatches, tpMatches, fpMatches, fnMatches, hplMentions)
        self.stop()
    }

    override def postStop() {
      super.postStop()
      latch.countDown()
    }
  }

  def decodeMatchOnlySegmentation(mentions: Seq[Mention], otherMentions: Seq[Seq[Mention]],
                                  id2cluster: HashMap[String, String], params: AlignParams,
                                  matchThreshold: Double): Seq[Mention] = {
    val latch = new CountDownLatch(numWorkers)
    val workers = Vector.fill(numWorkers)(
      actorOf(new ViterbiAlignmentMatchOnlyWorker(id2cluster, params, matchThreshold, latch)).start())
    for (b <- 0 until numBatches(mentions.size)) {
      workers(b % numWorkers) ! ProcessAlignMentions(mentions.slice(b * batchSize, (b + 1) * batchSize),
        otherMentions.slice(b * batchSize, (b + 1) * batchSize))
    }

    var total = 0
    var tp = 0
    var fp = 0
    var fn = 0
    val hplMentions = new ArrayBuffer[Mention]
    for (worker <- workers) {
      for (result <- worker.!!(FinishedMentions, MAXTIME).as[AlignPerfResult]) {
        total += result.total
        tp += result.tp
        fp += result.fp
        fn += result.fn
        hplMentions ++= result.labeledMentions
      }
    }
    latch.await()
    logger.info("#matches=" + total + " #tpMatches=" + tp + " #fpMatches=" + fp + " #fnMatches=" + fn)
    hplMentions
  }

  // 4. Paraphrase extraction
  def dumpParaphraseExtractions(rawRecords: Seq[Mention], rawTexts: Seq[Mention], blocker: AbstractBlocker,
                                id2fExample: HashMap[String, FeatMentionExample], countThresh: Double = 5) {
    val recordTable = new HashWeightVec[Seq[Int]]()
    for (r <- rawRecords) {
      val rx = id2fExample(r.id)
      val rxSegmentation = rx.trueSegmentation
      val rxWordFeatures = rx.featSeq
      for (i <- 0 until rxSegmentation.numSegments) {
        val rxSegment = rxSegmentation.segment(i)
        val rxPhrase = rxWordFeatures.slice(rxSegment.begin, rxSegment.end)
        recordTable.increment_!(rxPhrase, 1)
      }
    }
    val textTable = new HashWeightVec[Seq[Int]]()
    for (t <- rawTexts) {
      val tx = id2fExample(t.id)
      val txWordFeatures = tx.featSeq
      for (b <- 0 until tx.words.length) {
        for (e <- (b + 1) to tx.words.length) {
          val txPhrase = txWordFeatures.slice(b, e)
          textTable.increment_!(txPhrase, 1)
        }
      }
    }
    val phraseTranslateTable = new HashWeightVec[(Seq[Int], Seq[Int])]()
    val bipartiteWt = 1.0
    for (r <- rawRecords) {
      val rx = id2fExample(r.id)
      val rxSegmentation = rx.trueSegmentation
      val rxWordFeatures = rx.featSeq
      for (t <- rawTexts if blocker.isPair(r.id, t.id)) {
        // iterate over record fields & text segments
        val tx = id2fExample(t.id)
        val txWordFeatures = tx.featSeq
        for (i <- 0 until rxSegmentation.numSegments) {
          val rxSegment = rxSegmentation.segment(i)
          val rxPhrase = rxWordFeatures.slice(rxSegment.begin, rxSegment.end)
          for (b <- 0 until tx.words.length) {
            for (e <- (b + 1) to tx.words.length) {
              val txPhrase = txWordFeatures.slice(b, e)
              if (recordTable(rxPhrase) >= countThresh && textTable(txPhrase) >= countThresh)
                phraseTranslateTable.increment_!(rxPhrase -> txPhrase, bipartiteWt)
            }
          }
        }
      }
      print(".")
      System.out.flush()
    }
    println()
    val recordTableOut = new PrintWriter("record-phrase-table.txt")
    for ((phrase, count) <- recordTable if count >= countThresh) {
      recordTableOut.println(count + "\t" + phrase.map(wordFeatureIndexer(_)).mkString(" "))
    }
    recordTableOut.close()
    val textTableOut = new PrintWriter("text-phrase-table.txt")
    for ((phrase, count) <- textTable if count >= countThresh) {
      textTableOut.println(count + "\t" + phrase.map(wordFeatureIndexer(_)).mkString(" "))
    }
    textTableOut.close()
    val phraseTranslateTableOut = new PrintWriter("phrase-translate-table.txt")
    for ((phraseTranslation, count) <- phraseTranslateTable if count >= countThresh) {
      val recPhrase = phraseTranslation._1
      val txtPhrase = phraseTranslation._2
      phraseTranslateTableOut.println(count +
        "\t" + recPhrase.map(wordFeatureIndexer(_)).mkString(" ") +
        "\t" + txtPhrase.map(wordFeatureIndexer(_)).mkString(" "))
    }
    phraseTranslateTableOut.close()
  }

  // 5. Supervised alignment learning
  case class ProcessMatchExamples(exs: Seq[FeatVecAlignmentMentionExample]) extends LearnMessage

  case class SegmentMatchResult(stats: ProbStats, counts: Params) extends LearnMessage

  case class SegmentMatchSemiSupResult(stats: ProbStats, counts: ConstraintParams) extends LearnMessage

  trait SegmentationMatchWorker extends Actor {
    val stats = new ProbStats
    var progress = 0

    def iter: Int

    def unlabeledWeight: Double

    def params: Params

    def counts: Params

    def latch: CountDownLatch

    def updateProgress(done: Boolean = false) {
      progress += 1
      if (progress % 100 == 0 || done) logger.info("Processed " + progress + " mentions")
    }

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double)

    def receive = {
      case ProcessMatchExamples(exs) =>
        // perform expectation step on each mention
        exs.foreach(ex => {
          val stepSize = if (ex.isRecord) 1.0 else unlabeledWeight
          doInfer(ex, stepSize)
          updateProgress()
        })
      case FinishedMentions =>
        updateProgress(true)
        self.channel ! SegmentMatchResult(stats, counts)
        self.stop()
    }

    override def postStop() {
      super.postStop()
      latch.countDown()
    }
  }

  class CRFConstraintsSegmentationMatchWorker(val iter: Int, val params: Params,
                                              val unlabeledWeight: Double, val latch: CountDownLatch)
    extends SegmentationMatchWorker {
    val counts = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      new CRFMatchSegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector, ex, params, counts,
        InferSpec(0, 1, false, false, false, false, false, true, 1, stepSize),
        true, true).updateCounts
    }
  }

  class CRFExpectationsSegmentationMatchWorker(val iter: Int, val params: Params,
                                               val unlabeledWeight: Double, val latch: CountDownLatch)
    extends SegmentationMatchWorker {
    val counts = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      // constraints cost
      val truthInfer = new CRFMatchSegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector,
        ex, params, counts, InferSpec(0, 1, false, false, false, false, false, true, 1, 0),
        true, true)
      // expectations
      val predInfer = new CRFMatchSegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector,
        ex, params, counts, InferSpec(0, 1, false, false, false, false, false, true, 1, -stepSize),
        false, false)
      predInfer.updateCounts
      stats += (truthInfer.stats - predInfer.stats) * stepSize
    }
  }

  def learnSupervisedAlignParamsCRF(numIter: Int, examples: Seq[FeatVecAlignmentMentionExample], params: Params,
                                    textWeight: Double, invVariance: Double): Params = {
    // calculate constraints once
    val constraintLatch = new CountDownLatch(numWorkers)
    val constraintWorkers = Vector.fill(numWorkers)(
      actorOf(new CRFConstraintsSegmentationMatchWorker(0, params, textWeight, constraintLatch)).start())
    for (b <- 0 until numBatches(examples.size)) {
      constraintWorkers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
    }

    val constraints = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    constraintWorkers.foreach(worker => {
      for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchResult]) {
        // get work from each worker
        constraints.add_!(result.counts, 1)
      }
    })
    constraintLatch.await()
    // constraints.output(logger.info(_))

    val objective = new ACRFObjective[Params](params, invVariance) {
      val logger = Logger.getLogger("supervised-crf-align-trainer")

      def getValueAndGradient = {
        val expectationLatch = new CountDownLatch(numWorkers)
        val workers = Vector.fill(numWorkers)(
          actorOf(new CRFExpectationsSegmentationMatchWorker(0, params, textWeight, expectationLatch)).start())
        for (b <- 0 until numBatches(examples.size)) {
          workers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
        }

        val expectations = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
        val stats = new ProbStats()
        workers.foreach(worker => {
          for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchResult]) {
            // get work from each worker
            stats += result.stats
            expectations.add_!(result.counts, 1)
          }
        })
        expectationLatch.await()
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

  def newDefaultConstraintMatchInferencer(ex: FeatVecAlignmentMentionExample, params: Params, counts: Params,
                                          constraintParams: ConstraintParams, constraintCounts: ConstraintParams,
                                          ispec: InferSpec): CRFMatchSegmentationInferencer = {
    throw fail("Not implemented!!")
  }

  def newConstraintMatchInferencer(ex: FeatVecAlignmentMentionExample, params: Params, counts: Params,
                                   constraintParams: ConstraintParams, constraintCounts: ConstraintParams,
                                   ispec: InferSpec, doUpdate: Boolean): CRFMatchSegmentationInferencer = {
    throw fail("Not implemented!!")
  }

  // 6. Semi-supervised parameter learning
  trait SemiSupSegmentationMatchWorker extends Actor {
    val stats = new ProbStats
    var progress = 0

    def iter: Int

    def unlabeledWeight: Double

    def constraintParams: ConstraintParams

    def constraintCounts: ConstraintParams

    def params: Params

    def latch: CountDownLatch

    def updateProgress(done: Boolean = false) {
      progress += 1
      if (progress % 100 == 0 || done) logger.info("Processed " + progress + " mentions")
    }

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double)

    def receive = {
      case ProcessMatchExamples(exs) =>
        // perform expectation step on each mention
        exs.foreach(ex => {
          val stepSize = if (ex.isRecord) 1.0 else unlabeledWeight
          doInfer(ex, stepSize)
          updateProgress()
        })
      case FinishedMentions =>
        updateProgress(true)
        self.channel ! SegmentMatchSemiSupResult(stats, constraintCounts)
        self.stop()
    }

    override def postStop() {
      super.postStop()
      latch.countDown()
    }
  }

  class CRFSemiSupDefaultsSegmentationMatchWorker(val iter: Int, val constraintParams: ConstraintParams,
                                                  val params: Params, val latch: CountDownLatch)
    extends SemiSupSegmentationMatchWorker {
    val unlabeledWeight: Double = 1.0
    val constraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      val predInfer = newDefaultConstraintMatchInferencer(ex, params, params, constraintParams, constraintCounts,
        InferSpec(0, 1, false, ex.isRecord, false, false, false, true, 1, stepSize))
      predInfer.updateCounts
      stats -= predInfer.stats
    }
  }

  class CRFSemiSupSegmentationMatchWorker(val iter: Int, val constraintParams: ConstraintParams,
                                          val params: Params, val latch: CountDownLatch)
    extends SemiSupSegmentationMatchWorker {
    val unlabeledWeight: Double = 1.0
    val constraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      val predInfer = newConstraintMatchInferencer(ex, params, params, constraintParams, constraintCounts,
        InferSpec(0, 1, false, ex.isRecord, false, false, false, true, 1, -stepSize), false)
      predInfer.updateCounts
      stats -= predInfer.stats
    }
  }

  def learnSemiSupervisedConstraintParamsCRF(numIter: Int, examples: Seq[FeatVecAlignmentMentionExample],
                                             params: Params, constraintParams: ConstraintParams,
                                             invConstraintVariance: Double): ConstraintParams = {
    val defaultLatch = new CountDownLatch(numWorkers)
    val defaultWorkers = Vector.fill(numWorkers)(
      actorOf(new CRFSemiSupDefaultsSegmentationMatchWorker(0, constraintParams, params, defaultLatch)).start())
    for (b <- 0 until numBatches(examples.size)) {
      defaultWorkers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
    }

    val defaultConstraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)
    defaultWorkers.foreach(worker => {
      for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchSemiSupResult]) {
        defaultConstraintCounts.add_!(result.counts, 1)
      }
    })
    defaultLatch.await()
    defaultConstraintCounts.output(logger.info(_))

    val objective = new ACRFObjective[ConstraintParams](constraintParams, invConstraintVariance) {
      val logger = Logger.getLogger("semi-supervised-constraint-param-trainer")

      override def projectPoint(point: Array[Double]): Array[Double] = {
        positiveProjection.project(point)
        point
      }

      def getValueAndGradient: (ConstraintParams, ProbStats) = {
        val latch = new CountDownLatch(numWorkers)
        val workers = Vector.fill(numWorkers)(
          actorOf(new CRFSemiSupSegmentationMatchWorker(0, constraintParams, params, latch)).start())
        for (b <- 0 until numBatches(examples.size)) {
          workers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
        }

        val constraintExpectations = newConstraintParams(false, true, constraintFeatureIndexer)
        val stats = new ProbStats()
        // objective = b*lambda + log Z(lambda) + eps/2 * ||lambda||^2
        workers.foreach(worker => {
          for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchSemiSupResult]) {
            // get work from each worker
            stats += result.stats
            constraintExpectations.add_!(result.counts, 1)
          }
        })
        latch.await()
        forIndex(constraintParams.getWtVecs.length, k => {
          stats.logZ -= constraintParams.getWtVecs(k).dot(defaultConstraintCounts.getWtVecs(k))
        })
        constraintExpectations.add_!(defaultConstraintCounts, -1)
        (constraintExpectations, stats)
      }
    }

    val ls = new ArmijoLineSearchMinimizationAlongProjectionArc(new InterpolationPickFirstStep(1))
    val stop = new AverageValueDifference(1e-3)
    val optimizer = new ProjectedGradientDescent(ls)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished constraint alignment+segmentation learning only epoch=" +
          (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)

    constraintParams
  }

  class CRFSemiSupConstraintsSegmentationMatchWorker(val iter: Int, val params: Params,
                                                     val constraintParams: ConstraintParams,
                                                     val latch: CountDownLatch)
    extends SegmentationMatchWorker {
    val unlabeledWeight: Double = 1.0
    val counts = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    val constraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      newConstraintMatchInferencer(ex, params, counts, constraintParams, constraintCounts,
        InferSpec(0, 1, false, ex.isRecord, false, false, false, true, 1, stepSize), true).updateCounts
    }
  }

  class CRFSemiSupExpectationsSegmentationMatchWorker(val iter: Int, val params: Params,
                                                      val constraintParams: ConstraintParams,
                                                      val latch: CountDownLatch)
    extends SegmentationMatchWorker {
    val unlabeledWeight: Double = 1.0
    val counts = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    val constraintCounts = newConstraintParams(false, true, constraintFeatureIndexer)

    def doInfer(ex: FeatVecAlignmentMentionExample, stepSize: Double) {
      // val truthInfer = newConstraintMatchInferencer(ex, params, params, constraintParams, constraintCounts,
      //  InferSpec(0, 1, false, ex.isRecord, false, false, false, true, 1, 0), false)
      // expectations
      val predInfer = new CRFMatchSegmentationInferencer(labelIndexer, maxLengths, getAlignFeatureVector,
        ex, params, counts, InferSpec(0, 1, false, false, false, false, false, true, 1, -stepSize),
        false, false)
      predInfer.updateCounts
      // stats += (truthInfer.stats - predInfer.stats) * stepSize
      stats -= predInfer.stats * stepSize
    }
  }

  def learnSemiSupervisedAlignParamsCRF(numIter: Int, examples: Seq[FeatVecAlignmentMentionExample],
                                        params: Params, constraintParams: ConstraintParams,
                                        invVariance: Double): Params = {
    // calculate constraints once
    val constraintLatch = new CountDownLatch(numWorkers)
    val constraintWorkers = Vector.fill(numWorkers)(
      actorOf(new CRFSemiSupConstraintsSegmentationMatchWorker(0, params, constraintParams, constraintLatch)).start())
    for (b <- 0 until numBatches(examples.size)) {
      constraintWorkers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
    }

    val constraints = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
    constraintWorkers.foreach(worker => {
      for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchResult]) {
        // get work from each worker
        constraints.add_!(result.counts, 1)
      }
    })
    constraintLatch.await()

    val objective = new ACRFObjective[Params](params, invVariance) {
      val logger = Logger.getLogger("semi-supervised-crf-param-trainer")

      def getValueAndGradient: (Params, ProbStats) = {
        val expectationLatch = new CountDownLatch(numWorkers)
        val workers = Vector.fill(numWorkers)(
          actorOf(new CRFSemiSupExpectationsSegmentationMatchWorker(0, params, constraintParams,
            expectationLatch)).start())
        for (b <- 0 until numBatches(examples.size)) {
          workers(b % numWorkers) ! ProcessMatchExamples(examples.slice(b * batchSize, (b + 1) * batchSize))
        }

        val expectations = newParams(false, true, labelIndexer, featureIndexer, alignFeatureIndexer)
        val stats = new ProbStats()
        workers.foreach(worker => {
          for (result <- worker.!!(FinishedMentions, MAXTIME).as[SegmentMatchResult]) {
            // get work from each worker
            stats += result.stats
            expectations.add_!(result.counts, 1)
          }
        })
        expectationLatch.await()
        // add theta * E_q[f(x, y)] to objective (constraints term)
        forIndex(constraints.getWtVecs.length, (k: Int) => {
          stats.logZ += constraints.getWtVecs(k).dot(params.getWtVecs(k))
        })
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
        logger.info("*** finished semi-supervised alignment+segmentation learning only epoch=" +
          (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)

    params
  }

  def decodeSegmentationAlign(trueFilename: String, predFilename: String, examples: Seq[FeatVecAlignmentMentionExample],
                              decoder: (FeatVecAlignmentMentionExample) => Segmentation) {
    val trueOut = new PrintWriter(trueFilename)
    val predOut = new PrintWriter(predFilename)
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    for (ex <- examples if !ex.isRecord) {
      val predWidget = decoder(ex)
      val predSeg = adjustSegmentation(ex.words, predWidget)
      val trueSeg = adjustSegmentation(ex.words, ex.trueSegmentation)
      tokenPerf.add(trueSeg, predSeg)
      segmentPerf.add(trueSeg, predSeg)
      perLabelPerf.add(trueSeg, predSeg)
      predOut.println(SegmentationHelper.toFullString(ex.words, predSeg, labelIndexer(_)))
      trueOut.println(SegmentationHelper.toFullString(ex.words, trueSeg, labelIndexer(_)))
    }
    tokenPerf.output(logger.info(_))
    perLabelPerf.output(logger.info(_))
    segmentPerf.output(logger.info(_))
    trueOut.close()
    predOut.close()
  }
}