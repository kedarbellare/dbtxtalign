package cc.dbtxtalign.blocking

import collection.mutable.{HashMap, HashSet}
import com.mongodb.casbah.Imports._
import cc.dbtxtalign.Mention

/**
 * @author kedar
 */


trait AbstractBlocker {
  def orderedIds(id1: String, id2: String): (String, String) = {
    if (id1.hashCode() < id2.hashCode()) id1 -> id2 else id2 -> id1
  }

  def isPair(id1: String, id2: String): Boolean

  def numPairs: Int

  def getAllPairs: HashSet[(String, String)]

  protected def iterate(coll: MongoCollection, limit: Int) = {
    if (limit < 0) coll.find() else coll.find().take(limit)
  }

  def getRecall(cluster2ids: HashMap[String, Seq[String]], recordsColl: MongoCollection, textsColl: MongoCollection,
                maxRecords: Int, maxTexts: Int,
                printErrors: Boolean = false) = {
    var numPairs = 0
    var numFound = 0
    val validIds = new HashSet[String]
    for (dbo <- iterate(recordsColl, maxRecords); m = new Mention(dbo)) validIds += m.id
    for (dbo <- iterate(textsColl, maxTexts); m = new Mention(dbo)) validIds += m.id
    for (ids <- cluster2ids.values) {
      for (i <- 0 until ids.size; j <- i + 1 until ids.size) {
        if (validIds(ids(i)) && validIds(ids(j))) {
          if (isPair(ids(i), ids(j))) {
            numFound += 1
          } else if (printErrors) {
            val dbi1 = recordsColl.findOneByID(ids(i))
            val dbi2 = textsColl.findOneByID(ids(i))
            val dbj1 = recordsColl.findOneByID(ids(j))
            val dbj2 = textsColl.findOneByID(ids(j))
            val mi = new Mention({
              if (dbi1.isDefined) dbi1.get else dbi2.get
            })
            val mj = new Mention({
              if (dbj1.isDefined) dbj1.get else dbj2.get
            })
            println("\tmissed: [" + mi.isRecord + "][" + mi.words.mkString(" ") + "]\t[" + mj.isRecord + "][" +
              mj.words.mkString(" ") + "]")
          }
          numPairs += 1
        }
      }
    }
    println("hash: " + numFound + "/" + numPairs)
    (1.0 * numFound) / numPairs
  }
}