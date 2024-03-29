package cc.dbtxtalign

import blocking.{AbstractBlocker, PhraseHash, InvertedIndexBlocker, UnionIndexBlocker}
import mongo.KB
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.commons.Imports._
import cc.refectorie.user.kedarb.dynprog.types.FtrVec
import cc.refectorie.user.kedarb.dynprog.InferSpec
import org.riedelcastro.nurupo.Counting
import collection.mutable.{HashSet, ArrayBuffer, HashMap}
import org.apache.log4j.Logger
import io.Source
import java.io.{File, InputStream, PrintWriter}

/**
 * @author kedar
 */

object RexaKB extends KB("rexa-scratch",
  DBAlignConfig.get[String]("mongoHostname", "localhost"),
  DBAlignConfig.get[Int]("mongoPort", 27017))

trait ARexaAlign extends AbstractAlign {
  val kb = RexaKB

  val recordsColl = kb.getColl("records")
  val textsColl = kb.getColl("texts")
  val featuresColl = kb.getColl("features")
  val featureVectorsColl = kb.getColl("featureVectors")

  val YEAR = "(19|20)\\d\\d[a-z]?"
  val REFMARKER = "\\[[A-Za-z]*\\d+\\]"
  val INITIALS = "[A-Z]\\."
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"

  def simplify(s: String): String = {
    if (s.matches(YEAR)) "$year$"
    else if (s.matches(REFMARKER)) "$refmarker$"
    else if (s.matches(INITIALS)) "$initials$"
    else if (s.toLowerCase.matches(MONTH)) "$month$"
    else if (s.toLowerCase.matches(DOTW)) "$day$"
    else if (s.matches("\\(" + YEAR + "\\)")) "$yearbraces$"
    else s.replaceAll("\\d", "0").toLowerCase
  }

  def isPossibleEnd(j: Int, words: Seq[String]): Boolean = {
    val endsOnPunc = "^.*[^A-Za-z0-9\\-]$"
    val startsWithPunc = "^[^A-Za-z0-9\\-].*$"
    val endsWithAlpha = "^.*[A-Za-z]$"
    val endsWithNum = "^.*[0-9]$"
    val startsWithAlpha = "^[A-Za-z].*$"
    val startsWithNum = "^[0-9].*$"
    val endsOnSpecial = "^(and|et\\.?|vol\\.?|no\\.?|pp\\.?|pages)$"
    // info("calling isEnd('" + words(j - 1) + "'): " + words.mkString(" "))
    if (j == 0) false
    else j == words.length ||
      words(j - 1).matches(endsOnPunc) || // word ends on punctuation
      words(j).matches(startsWithPunc) || // words begins with punctuation
      words(j - 1).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      words(j).toLowerCase.matches(endsOnSpecial) || // "<s>X</s> and <s>Y</s>"
      (words(j - 1).matches(endsWithAlpha) && words(j).matches(startsWithNum)) || // alpha -> num
      (words(j - 1).matches(endsWithNum) && words(j).matches(startsWithAlpha)) // num -> alpha
  }

  def toFeatExample(m: Mention) = {
    val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => isPossibleEnd(j, m.words))
    val features = featuresColl.findOneByID(m.id).get.as[BasicDBList]("features").toArray.map(_.toString)
    new FeatMentionExample(m.id, m.isRecord, m.words, possibleEnds,
      features.map(wordFeatureIndexer.indexOf_!(_)), getSegmentationAndMaxLength_!(m, labelIndexer, maxLengths))
  }

  def toFeatVecExample(m: Mention) = {
    val possibleEnds = mapIndex(m.words.length + 1, (j: Int) => isPossibleEnd(j, m.words))
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
    logger.info("Starting blocking ...")
    val authorIndex1 = new InvertedIndexBlocker(500, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("author"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    }, maxRecords, maxTexts)
    logger.info("Finished indexing author ...")
    logger.info("#author1Pairs=" + authorIndex1.numPairs + " recall=" + authorIndex1.getRecall(cluster2ids, recordsColl, textsColl, maxRecords, maxTexts))
    val titleIndex1 = new InvertedIndexBlocker(500, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("title"), 2)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 2)
    }, maxRecords, maxTexts)
    logger.info("Finished indexing title ...")
    logger.info("#title1Pairs=" + titleIndex1.numPairs + " recall=" + titleIndex1.getRecall(cluster2ids, recordsColl, textsColl, maxRecords, maxTexts))
    val unionIndex1 = new UnionIndexBlocker(Seq(authorIndex1, titleIndex1), false)
    logger.info("Finished blocking ...")
    logger.info("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, recordsColl, textsColl, maxRecords, maxTexts, false))

    unionIndex1
  }
}

object RexaMongoLoader extends ARexaAlign {
  def main(args: Array[String]) {
    recordsColl.drop()
    textsColl.drop()

    FileHelper.loadRawMentions(kb, true, args(0), "records")
    FileHelper.loadRawMentions(kb, false, args(1), "texts")
  }
}

object RexaFeatureSequenceLoader extends ARexaAlign {
  def main(args: Array[String]) {
    val writer = new PrintWriter("rexa_simplified.txt")
    featuresColl.dropCollection()
    for (dbo <- recordsColl.find() ++ textsColl.find(); m = new Mention(dbo)) {
      val builder = MongoDBObject.newBuilder
      val features = m.words.map(simplify(_))
      builder += "_id" -> m.id
      builder += "features" -> features
      featuresColl += builder.result()
      // output features
      if (!m.isRecord) {
        writer.println(features.mkString("\t"))
      }
    }
    writer.close()
  }
}

object RexaFeatureVectorSequenceLoader extends ARexaAlign {
  def main(args: Array[String]) {
    import RexaCitationFeatures._

    val simplified2path = FileHelper.getMapping2to1(args(0))
    val writer = new PrintWriter("rexa_features.txt")
    featureVectorsColl.dropCollection()
    for (dbo <- recordsColl.find() ++ textsColl.find(); m = new Mention(dbo)) {
      val builder = MongoDBObject.newBuilder
      val lookAheadFeatures = Array.fill(m.words.length)(new ArrayBuffer[String])
      val features = mapIndex(m.words.length, (ip: Int) => {
        val word = m.words(ip)
        val simplified = simplify(word)
        val feats = new ArrayBuffer[String]
        feats += "SIMPLIFIED=" + simplified
        if (simplified2path.contains(simplified))
          feats += "CLUSTERPATH=" + simplified2path(simplified)
        for ((key, featfn) <- prefixToFtrFns) {
          if (featfn(word).isDefined)
            feats += key
          if (!simplified.matches("\\$.*\\$") && featfn(simplified).isDefined)
            feats += "SIMPLIFIED:" + key
        }
        // match trie and add to look ahead features
        for ((key, lex) <- prefixToTrieLexicon) {
          val endip = lex.endIndexOf(m.words, ip)
          if (endip >= ip) {
            forIndex(ip, endip + 1, k => {
              lookAheadFeatures(k) += "PHRASEMATCH=" + key
            })
          }
        }

        // add look-ahead features
        feats ++= lookAheadFeatures(ip)
        feats.toSeq
      })
      builder += "_id" -> m.id
      builder += "featureVectors" -> features
      featureVectorsColl += builder.result()
      // output features
      if (!m.isRecord) {
        writer.println(features.mkString("\n"))
        writer.println()
      }
    }
    writer.close()
  }
}

object RexaApp extends ARexaAlign {
  def main(args: Array[String]) {
    val rawRecords = recordsColl.map(new Mention(_)).take(maxRecords).toArray
    val rawTexts = textsColl.map(new Mention(_)).take(maxTexts).toArray
    val rawMentions = rawRecords ++ rawTexts
    logger.info("#records=" + rawRecords.size + " #texts=" + rawTexts.size)

    val id2mention = new HashMap[String, Mention]
    val id2fExample = new HashMap[String, FeatMentionExample]
    val id2fvecExample = new HashMap[String, FeatVecMentionExample]
    val mentionCounting = new Counting(1000, cnt => logger.info("Processed " + cnt + "/" + rawMentions.size))
    for (m <- mentionCounting(rawMentions)) {
      id2mention(m.id) = m
      id2fExample(m.id) = toFeatExample(m)
      id2fvecExample(m.id) = toFeatVecExample(m)
      token2WordIndices(m.id) = m.words.map(wordIndexer.indexOf_!(_))
      tokenSimilarityIndex.index(m.id, m.words)
      bigramSimilarityIndex.index(m.id, m.words)
      trigramSimilarityIndex.index(m.id, m.words)
    }

    val id2cluster = FileHelper.getMapping1to2(args(0))
    val cluster2ids = getClusterToIds(id2cluster)

    // 1. calculate candidate pairs using author and title
    val blocker = getBlocker(cluster2ids)

    // 2. Find for the set of records that are candidate matches for each text
    logger.info("#maxMatched=" + getMaxRecordsMatched(rawTexts, rawRecords, blocker))

    // 3. Segment HMM baseline
    var hmmParams = newSegmentParams(true, true, labelIndexer, wordFeatureIndexer)
    hmmParams.setUniform_!
    hmmParams.normalize_!(1e-2)
    hmmParams = learnEMSegmentParamsHMM(20, rawMentions, hmmParams, 1e-2, 1e-2)
    decodeSegmentation("rexa.hmm.true.txt", "rexa.hmm.pred.txt", rawMentions, (m: Mention) => {
      val ex = id2fExample(m.id)
      val inferencer = new HMMSegmentationInferencer(labelIndexer, maxLengths, ex, hmmParams, hmmParams,
        InferSpec(0, 1, false, false, true, false, true, false, 1, 0))
      inferencer.bestWidget
    })
  }
}