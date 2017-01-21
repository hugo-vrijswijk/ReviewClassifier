import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

/**
  * Tokenizes
  */
object ReviewParser {
  def parseAll(rows: Iterable[Row]): Iterable[Review] = rows.flatMap(parse)

  def parse(row: Row): ArrayBuffer[Review] = {

  }
}

case class Review(docId: String, text: Seq[String], sentiment: Boolean)
