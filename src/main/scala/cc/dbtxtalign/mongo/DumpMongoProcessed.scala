package cc.dbtxtalign.mongo

import com.mongodb.casbah.Imports._
import java.io.PrintWriter

/**
 * @author kedar
 */


object DumpMongoProcessed {
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Usage: " + this.getClass.getSimpleName +
        " <db_name> <coll_name> <record_outfile> <text_outfile> <cluster_outfile>")
      return
    }
    val dbName = args(0)
    val collName = args(1)
    val mongoConn = MongoConnection()
    val mentions = mongoConn(dbName)(collName)
    val recordWriter = new PrintWriter(args(2))
    val textWriter = new PrintWriter(args(3))
    val clusterWriter = new PrintWriter(args(4))
    mentions.foreach(m => {
      val id = m._id.get.toString
      val isRecord = m.as[Boolean]("isRecord")
      val trueCluster = m.getAs[String]("trueCluster")
      val words = m.as[BasicDBList]("words").toArray()
      val labels = m.as[BasicDBList]("trueLabels").toArray()
      val writer = if (isRecord) recordWriter else textWriter
      for (clust <- trueCluster) clusterWriter.println(id + "\t" + clust)
      writer.println(id)
      writer.println(words.mkString("\t"))
      writer.println(labels.mkString("\t"))
      writer.println()
    })
    recordWriter.close()
    textWriter.close()
    clusterWriter.close()
  }
}