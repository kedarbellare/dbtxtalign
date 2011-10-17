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
    val authorIndex1 = new InvertedIndexBlocker(500, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("author"), 1)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 1)
    })
    val titleIndex1 = new InvertedIndexBlocker(500, recordsColl, textsColl, {
      m: Mention => PhraseHash.ngramWordHash(m.extractTrueWordsFor("title"), 2)
    }, {
      m: Mention => PhraseHash.ngramWordHash(m.words, 2)
    })
    val unionIndex1 = new UnionIndexBlocker(Seq(authorIndex1, titleIndex1), false)

    // recall of hashes
    logger.info("#author1Pairs=" + authorIndex1.numPairs + " recall=" + authorIndex1.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#title1Pairs=" + titleIndex1.numPairs + " recall=" + titleIndex1.getRecall(cluster2ids, recordsColl, textsColl))
    logger.info("#unionPairs=" + unionIndex1.numPairs + " recall=" + unionIndex1.getRecall(cluster2ids, recordsColl, textsColl, false))

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

class RexaDict(val name: String, val toLC: Boolean = true) {
  val set = new HashSet[String]

  def add(s: String): Unit = set += {
    if (toLC) s.toLowerCase else s
  }

  def contains(s: String): Boolean = set.contains(if (toLC) s.toLowerCase else s)

  override def toString = name + " :: " + set.mkString("\n")
}

object RexaDict {
  val logger = Logger.getLogger(getClass.getSimpleName)

  def fromFile(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromFile(filename), toLC)
  }

  def fromResource(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)), toLC)
  }

  def fromResourceOrFile(filename: String, toLC: Boolean = true): RexaDict = {
    try {
      val is: InputStream = getClass.getClassLoader.getResourceAsStream(filename)
      if (is != null) fromSource(filename, Source.fromInputStream(is), toLC)
      else if (new File(filename).exists) {
        logger.warn("Couldn't find file %s in classpath! Using relative path instead.".format(filename))
        fromSource(filename, Source.fromFile(filename), toLC)
      } else {
        logger.warn("Couldn't find file %s in classpath or relative path! Returning empty dict.".format(filename))
        return new RexaDict(new File(filename).getName, toLC)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        return new RexaDict(new File(filename).getName, toLC)
    }
  }

  def fromSource(filename: String, source: Source, toLC: Boolean = true): RexaDict = {
    val name = new File(filename).getName
    val dict = new RexaDict(name, toLC)
    for (line <- source.getLines) dict.add(line)
    dict
  }
}

object RexaFeatureVectorSequenceLoader extends ARexaAlign {
  val prefixToFtrFns = new HashMap[String, String => Option[String]]
  val LEX_ROOT = "citations/lexicons/"

  def LexiconResource(name: String, filename: String, toLC: Boolean = true): Unit = {
    val dict = RexaDict.fromResourceOrFile(LEX_ROOT + filename, toLC)
    prefixToFtrFns("LEXICON=" + name) = {
      s: String =>
        val smod = (if (toLC) s.toLowerCase else s).replaceAll("[^A-Za-z0-9]+", " ").trim()
        if (smod.length > 3 && dict.contains(smod)) Some("") else None
    }
  }

  def regex(name: String, pattern: String) {
    prefixToFtrFns("REGEX=" + name) = {
      s: String => if (s.matches(pattern)) Some("") else None
    }
  }

  def RegexMatcher(name: String, pattern: String) {
    regex(name, pattern)
  }

  val CAPS = "[A-Z]"
  val ALPHA = "[A-Za-z]"
  val ALPHANUM = "[A-Za-z0-9]"
  val NUM = "[0-9]"
  val PUNC = "[,\\.;:?!()\"'`]"

  // define feature functions
  LexiconResource("DBLPTITLESTARTHIGH", "title.start.high", false)
  LexiconResource("DBLPTITLEHIGH", "title.high")
  LexiconResource("DBLPAUTHORFIRST", "author-first", false)
  LexiconResource("DBLPAUTHORLAST", "author-last", false)
  LexiconResource("CONFABBR", "conferences.abbr", false)
  LexiconResource("PLACES", "places")

  LexiconResource("INSTITUTELEX", "institute-words.txt", false)
  LexiconResource("FirstHighest", "personname/ssdi.prfirsthighest")
  LexiconResource("FirstHigh", "personname/ssdi.prfirsthigh")
  LexiconResource("LastHighest", "personname/ssdi.prlasthighest")
  LexiconResource("LastHigh", "personname/ssdi.prlasthigh")
  LexiconResource("Honorific", "personname/honorifics")
  LexiconResource("NameSuffix", "personname/namesuffixes")
  LexiconResource("NameParticle", "personname/name-particles")
  LexiconResource("Nickname", "personname/nicknames")
  LexiconResource("Day", "days")
  LexiconResource("Month", "months")
  LexiconResource("StateAbbrev", "state_abbreviations")
  LexiconResource("Stopword", "stopwords")

  RegexMatcher("CONTAINSDOTS", "[^\\.]*\\..*")
  RegexMatcher("CONTAINSCOMMA", ".*,.*")
  RegexMatcher("CONTAINSDASH", ALPHANUM + "+-" + ALPHANUM + "*")
  RegexMatcher("ACRO", "[A-Z][A-Z\\.]*\\.[A-Z\\.]*")

  RegexMatcher("URL1", "www\\..*|https?://.*|ftp\\..*|.*\\.edu/?.*")

  // patterns involving numbers
  RegexMatcher("PossiblePage", "[0-9]+\\s*[\\-{#]+\\s*[0-9]+")
  RegexMatcher("PossibleVol", "[0-9][0-9]?\\s*\\([0-9]+\\)")
  RegexMatcher("5+digit", "[0-9][0-9][0-9][0-9][0-9]+")
  RegexMatcher("HasDigit", ".*[0-9].*")
  RegexMatcher("AllDigits", "[0-9]+")

  RegexMatcher("ORDINAL1", "(?ii)[0-9]+(?:st|nd|rd|th)")
  RegexMatcher("ORDINAL2", ("(?ii)(?:"
    + "[Ff]irst|[Ss]econd|[Tt]hird|[Ff]ourth|[Ff]ifth|[Ss]ixth|[Ss]eventh|[Ee]ighth|[Nn]inth|[Tt]enth"
    + "|[Ee]leventh|[Tt]welfth|[Tt]hirteenth|[Ff]ourteenth|[Ff]ifteenth|[Ss]ixteenth"
    + "|[Ss]eventeenth|[Ee]ighteenth|[Nn]ineteenth"
    + "|[Tt]wentieth|[Tt]hirtieth|[Ff]ou?rtieth|[Ff]iftieth|[Ss]ixtieth|[Ss]eventieth"
    + "|[Ee]ightieth|[Nn]ine?tieth|[Tt]wentieth|[Hh]undredth"
    + ")"))

  // Punctuation
  RegexMatcher("Punc", PUNC)
  RegexMatcher("LeadQuote", "[\"'`]")
  RegexMatcher("EndQuote", "[\"'`][^s]?")
  RegexMatcher("MultiHyphen", "\\S*-\\S*-\\S*")
  RegexMatcher("ContainsPunc", "[\\-,\\:\\;]")
  RegexMatcher("StopPunc", "[\\!\\?\\.\"\']")

  // Character-based
  RegexMatcher("LONELYINITIAL", CAPS + "\\.")
  RegexMatcher("CAPLETTER", CAPS)
  RegexMatcher("ALLCAPS", CAPS + "+")

  // Field specific
  RegexMatcher("PossibleEditor", "(ed\\.|editor|editors|eds\\.)")
  RegexMatcher("PossiblePage", "(pp\\.|page|pages)")
  RegexMatcher("PossibleVol", "(no\\.|vol\\.?|volume)")
  RegexMatcher("PossibleInstitute", "(University|Universite|Universiteit|Univ\\.?|Dept\\.?|Institute|Corporation|Department|Laboratory|Laboratories|Labs)")

  def main(args: Array[String]) {
    val simplified2path = FileHelper.getMapping2to1(args(0))
    val writer = new PrintWriter("rexa_features.txt")
    featureVectorsColl.dropCollection()
    for (dbo <- recordsColl.find() ++ textsColl.find(); m = new Mention(dbo)) {
      val builder = MongoDBObject.newBuilder
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
    val rawRecords = recordsColl.map(new Mention(_)).toArray
    val rawTexts = textsColl.map(new Mention(_)).toArray
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