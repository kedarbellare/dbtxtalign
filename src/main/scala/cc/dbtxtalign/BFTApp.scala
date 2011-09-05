package cc.dbtxtalign

import blocking.{UnionIndexBlocker, InvertedIndexBlocker, PhraseHash}
import io.Source
import collection.mutable.{HashSet, HashMap, ArrayBuffer}
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation

/**
 * @author kedar
 */

object FileHelper {
  def getRawMentions(isRecord: Boolean, filename: String, simplify: (String) => String = identity(_)): Seq[Mention] = {
    val buff = new ArrayBuffer[Mention]
    val lineIter = Source.fromFile(filename).getLines()
    while (lineIter.hasNext) {
      val id = lineIter.next()
      val words = lineIter.next().split("\t").map(simplify(_))
      val bioLabels = lineIter.next().split("\t")
      // empty line
      lineIter.next()
      buff += new Mention(id, isRecord, words, bioLabels)
    }
    buff.toSeq
  }

  def getMentionClusters(filename: String): HashMap[String, String] = {
    val id2cluster = new HashMap[String, String]
    Source.fromFile(filename).getLines().foreach(line => {
      val parts = line.split("\t")
      id2cluster(parts(0)) = parts(1)
    })
    id2cluster
  }
}

object BFTApp {
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

  def main(args: Array[String]) {
    val labelIndexer = new Indexer[String]
    val featureIndexer = new Indexer[String]
    val maxLengthMap = new HashMap[Int, Int]

    val rawRecords = FileHelper.getRawMentions(true, args(0), simplify(_))
    val rawTexts = FileHelper.getRawMentions(false, args(1), simplify(_))
    val rawMentions = rawRecords ++ rawTexts
    println("#records=" + rawRecords.size + " #texts=" + rawTexts.size)
    val id2mention = new HashMap[String, Mention]
    val id2example = new HashMap[String, FeatMentionExample]
    for (m <- rawMentions) {
      val featSeq = m.words.map(featureIndexer.indexOf_!(_))
      val trueSeg = Segmentation.fromBIO(m.trueBioLabels, labelIndexer.indexOf_!(_))
      if (m.isRecord) {
        trueSeg.segments.foreach(s => {
          maxLengthMap(s.label) = math.max(s.end - s.begin, maxLengthMap.getOrElse(s.label, 0))
        })
      }
      id2mention(m.id) = m
      id2example(m.id) = new FeatMentionExample(m.id, m.isRecord, m.words, featSeq, trueSeg)
    }

    val maxLengths = Array.ofDim[Int](labelIndexer.size)
    for (l <- 0 until labelIndexer.size) maxLengths(l) = maxLengthMap.getOrElse(l, 1)
    // update max lengths if needed
    
    val id2cluster = FileHelper.getMentionClusters(args(2))
    val cluster2ids = new HashMap[String, Seq[String]]
    for ((id, cluster) <- id2cluster) cluster2ids(cluster) = cluster2ids.getOrElse(cluster, Seq.empty[String]) ++ Seq(id)
    
    // 1. Calculate candidate pairs using hotelname and localarea
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
    println("#name1Pairs=" + nameIndex1.numPairs + " recall=" + nameIndex1.getRecall(cluster2ids, id2mention))
    println("#name2Pairs=" + nameIndex2.numPairs + " recall=" + nameIndex2.getRecall(cluster2ids, id2mention))
    println("#area1Pairs=" + areaIndex1.numPairs + " recall=" + areaIndex1.getRecall(cluster2ids, id2mention))
    println("#area2Pairs=" + areaIndex2.numPairs + " recall=" + areaIndex2.getRecall(cluster2ids, id2mention))
    println("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, id2mention, true))

    // 2. Find for the set of records that are candidate matches for each text
    var maxRecordsMatched = 0
    for (m <- rawTexts) {
      // println("text[" + m.id + "][cluster=" + id2cluster.getOrElse(m.id, "") + "]: " + m.words.mkString(" "))
      var numRecords = 0
      for (r <- rawRecords if unionIndex1.isPair(m.id, r.id)) {
        numRecords += 1
        // println("\trecord[" + r.id + "][cluster=" + id2cluster.getOrElse(r.id, "") + "]: " + r.words.mkString(" "))
      }
      if (maxRecordsMatched < numRecords) maxRecordsMatched = numRecords
      // println("\t#recordsMatched=" + numRecords)
    }
    println("#maxMatched=" + maxRecordsMatched)
  }
}