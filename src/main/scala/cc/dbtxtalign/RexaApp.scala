package cc.dbtxtalign

import blocking.{PhraseHash, InvertedIndexBlocker}
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import collection.mutable.HashMap
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */


object RexaApp {
  def main(args: Array[String]) {
    val labelIndexer = new Indexer[String]
    // val featureIndexer = new Indexer[String]
    val maxLengthMap = new HashMap[Int, Int]

    val rawRecords = FileHelper.getRawMentions(true, args(0))
    val rawTexts = FileHelper.getRawMentions(false, args(1))
    val rawMentions = rawRecords ++ rawTexts
    println("#records=" + rawRecords.size + " #texts=" + rawTexts.size)
    val id2mention = new HashMap[String, Mention]
    // val id2example = new HashMap[String, FeatMentionExample]
    var numMentions = 0
    val maxMentions = rawMentions.size
    for (m <- rawMentions) {
      // val featSeq = m.words.map(featureIndexer.indexOf_!(_))
      val trueSeg = Segmentation.fromBIO(m.trueBioLabels, labelIndexer.indexOf_!(_))
      if (m.isRecord) {
        trueSeg.segments.foreach(s => {
          maxLengthMap(s.label) = math.max(s.end - s.begin, maxLengthMap.getOrElse(s.label, 0))
        })
      }
      id2mention(m.id) = m
      // id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, featSeq, trueSeg)
      numMentions += 1
      if (numMentions % 1000 == 0) {
        println("Processed " + numMentions + "/" + maxMentions)
      }
    }

    val maxLengths = Array.ofDim[Int](labelIndexer.size)
    for (l <- 0 until labelIndexer.size) maxLengths(l) = maxLengthMap.getOrElse(l, 1)
    // update max lengths if needed

    val id2cluster = FileHelper.getMentionClusters(args(2))
    val cluster2ids = new HashMap[String, Seq[String]]
    for ((id, cluster) <- id2cluster) cluster2ids(cluster) = cluster2ids.getOrElse(cluster, Seq.empty[String]) ++ Seq(id)

    // 1. calculate candidate pairs using author and title
    val authorIndex1 = new InvertedIndexBlocker(50, rawMentions, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("author"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    })

    // recall of hash1
    println("#author1Pairs=" + authorIndex1.numPairs + " recall=" + authorIndex1.getRecall(cluster2ids, id2mention))
  }
}