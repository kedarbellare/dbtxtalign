package cc.dbtxtalign

import io.Source
import mongo.KB
import collection.mutable.{HashMap, ArrayBuffer}
import org.apache.log4j.Logger
import org.riedelcastro.nurupo.{Config, Util}
import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

object FileHelper {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  val debugNumMentions = 1000

  def loadRawMentions(kb: KB, isRecord: Boolean, filename: String, collName: String) {
    val coll = kb.getColl(collName)
    val lineIter = Source.fromFile(filename).getLines()
    var numMentions = 0
    while (lineIter.hasNext) {
      val builder = MongoDBObject.newBuilder
      builder += "_id" -> lineIter.next()
      builder += "isRecord" -> isRecord
      builder += "source" -> filename
      builder += "words" -> lineIter.next().split("\t")
      builder += "bioLabels" -> lineIter.next().split("\t")
      // empty line
      lineIter.next()
      coll += builder.result()
      numMentions += 1
      if (numMentions % debugNumMentions == 0) logger.info("Loaded #mentions=" + numMentions)
    }
    logger.info("Finished loading #mentions=" + numMentions)
  }

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
      if (buff.size % debugNumMentions == 0) logger.info("Loaded #mentions=" + buff.size)
    }
    logger.info("Finished loading #mentions=" + buff.size)
    buff.toSeq
  }

  private def getMapping(filename: String, order: (String, String) => (String, String)): HashMap[String, String] = {
    val ret = new HashMap[String, String]
    Source.fromFile(filename).getLines().foreach(line => {
      val parts = line.split("\t")
      val (key, value) = order(parts(0), parts(1))
      ret(key) = value
    })
    ret
  }

  def getMapping1to2(filename: String): HashMap[String, String] = {
    getMapping(filename, (s1: String, s2: String) => (s1, s2))
  }

  def getMapping2to1(filename: String): HashMap[String, String] = {
    getMapping(filename, (s1: String, s2: String) => (s2, s1))
  }
}

object DBAlignConfig extends Config(Util.getStreamFromClassPathOrFile("dbie.properties"))
