package cc.dbtxtalign

import blocking.AbstractBlocker
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Indexer}
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author kedar
 */


trait AbstractAlign {
  def simplify(s: String): String

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
    val otherLabelIndex = indexer.indexOf_!("O")
    val trueSeg = getSegmentation_!(m, indexer)
    // default length is 1
    while (indexer.size != maxLengths.size) maxLengths += 1
    // accumulate max lengths overall
    // TODO (cheating oracle!)
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
}