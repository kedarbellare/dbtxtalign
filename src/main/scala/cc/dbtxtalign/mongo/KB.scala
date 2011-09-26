package cc.dbtxtalign.mongo

import com.mongodb.casbah.MongoConnection

/**
 * @author kedarb
 * @since 3/25/11
 */

class KB(val dbName: String, val hostname: String = "localhost", val port: Int = 27017) {
  val mongoConn = MongoConnection(hostname, port)

  def getColl(name: String) = {
    val coll = mongoConn(dbName)(name)
    coll
  }

  def getCollWithIndices(name: String, attrs: Seq[String]) = {
    val coll = getColl(name)
    attrs.foreach(coll.ensureIndex(_))
    coll
  }
}