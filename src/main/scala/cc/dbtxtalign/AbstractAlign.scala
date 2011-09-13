package cc.dbtxtalign

import blocking.AbstractBlocker
import collection.mutable.{ArrayBuffer, HashMap}
import java.io.PrintWriter
import params.{EmissionParams, TransitionParams, SegmentParams}
import cc.refectorie.user.kedarb.dynprog.types.{ParamUtils, FtrVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.InferSpec
import cc.refectorie.user.kedarb.dynprog.segment._
import org.apache.log4j.Logger

/**
 * @author kedar
 */


trait AbstractAlign {
  val wordIndexer = new Indexer[String]
  val labelIndexer = new Indexer[String]
  val wordFeatureIndexer = new Indexer[String]
  val featureIndexer = new Indexer[String]
  val maxLengths = new ArrayBuffer[Int]
  val otherLabelIndex = labelIndexer.indexOf_!("O")

  def logger: Logger

  def simplify(s: String): String
  
  def L = labelIndexer.size

  def W = wordIndexer.size

  def WF = wordFeatureIndexer.size

  def F = featureIndexer.size

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

  def getBlocker(rawMentions: Seq[Mention], id2mention: HashMap[String, Mention],
                 cluster2ids: HashMap[String, Seq[String]]): AbstractBlocker

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

  // Learning functions
  // 1. Segment HMM trained with EM
  def learnEMSegmentParamsHMM(numIter: Int, examples: Seq[FeatMentionExample],
                              unlabeledWeight: Double, smoothing: Double): SegmentParams = {
    var segparams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    segparams.setUniform_!
    segparams.normalize_!(smoothing)

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

  def decodeSegmentParamsHMM(trueFilename: String, predFilename: String,
                             examples: Seq[FeatMentionExample], segparams: SegmentParams) {
    val trueOut = new PrintWriter(trueFilename)
    val predOut = new PrintWriter(predFilename)
    val segmentPerf = new SegmentSegmentationEvaluator("textSegEval", labelIndexer)
    val tokenPerf = new SegmentLabelAccuracyEvaluator("textLblEval")
    val perLabelPerf = new SegmentPerLabelAccuracyEvaluator("textPerLblEval", labelIndexer)
    for (ex <- examples if !ex.isRecord) {
      val inferencer = new HMMSegmentationInferencer(labelIndexer, maxLengths, ex, segparams, segparams,
        InferSpec(0, 1, false, ex.isRecord, true, false, true, false, 1, 0))
      val predSeg = adjustSegmentation(ex.words, inferencer.bestWidget)
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