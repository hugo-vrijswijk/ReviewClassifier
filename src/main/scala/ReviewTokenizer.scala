import java.io.FileReader

import org.apache.lucene.analysis.en.{EnglishAnalyzer, EnglishMinimalStemFilter}
import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.synonym.{SynonymGraphFilter, WordnetSynonymParser}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{CharArraySet, LowerCaseFilter}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Tokenizes reviews (cuts it up) and wraps it in a Review object
  */
object ReviewTokenizer {
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  def parseAll(rows: Dataset[Row]): Dataset[Review] = rows.map(parse)

  def parse(row: Row): Review = {
    val id = row.getAs[String]("id")
    val reviewText = row.getAs[String]("review")
    val sentiment = row.getAs[String]("sentiment").toDouble
    val tokenizedText = tokenizeText(reviewText)

    Review(id, tokenizedText, sentiment)
  }

  def tokenizeText(text: String): Seq[String] = {

    val analyzer =
        new ShingleFilter(
            new EnglishMinimalStemFilter(
            new EnglishAnalyzer().tokenStream("contents", text)
          ), 4)

    val term = analyzer.addAttribute(classOf[CharTermAttribute])
    analyzer.reset()
    val tokenizedText = mutable.ArrayBuffer.empty[String]

    while (analyzer.incrementToken()) {
      tokenizedText += term.toString
    }
    analyzer.close()
    analyzer.end()

    tokenizedText
  }
}

case class Review(id: String, words: Seq[String], sentiment: Double)
