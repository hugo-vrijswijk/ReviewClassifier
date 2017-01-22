import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.Version
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Tokenizes a review (cuts it up) and wraps it in a Review object
  */
object ReviewTokenizer {
  val LuceneVersion = Version.LATEST

  def parseAll(rows: RDD[Row]): RDD[Review] = rows.map(parse)

  def parse(row: Row): Review = {
    val id = row.getAs[String]("id")
    val reviewText = row.getAs[String]("review")
    val sentiment = extractSentiment(row)

    val analyzer = new EnglishAnalyzer()
    val tokenStream = analyzer.tokenStream("contents", reviewText)

    val term = tokenStream.addAttribute(classOf[CharTermAttribute])
    tokenStream.reset()

    val tokenizedText = mutable.ArrayBuffer.empty[String]

    while (tokenStream.incrementToken()) {
      tokenizedText += term.toString
    }

    tokenStream.close()
    tokenStream.end()

    Review(id, tokenizedText, actualSentiment = sentiment)
  }

  def extractSentiment(row: Row): Option[Boolean] = {
    try {
      Option(intToBool(row.getAs[String]("sentiment")))
    } catch {
      case _: UnsupportedOperationException | _: IllegalArgumentException => None
    }
  }

  def intToBool(num: String): Boolean = num match {
    case "1" => true
    case _ => false
  }
}

case class Review(docId: String, terms: Seq[String], actualSentiment: Option[Boolean] = None, predictedSentiment: Option[Boolean] = None)
