import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.Version
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Tokenizes reviews (cuts it up) and wraps it in a Review object
  */
object ReviewTokenizer {
  val LuceneVersion = Version.LATEST
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()
  import spark.implicits._

  def parseAll(rows: Dataset[Row]): Dataset[Review] = rows.map(parse)

//    val tokenizedDs = new Tokenizer()
//    .setInputCol("review")
//      .setOutputCol("words")
//      .transform(rows).select("id", "words", "sentiment")
//
//    tokenizedDs.withColumn("sentimentTmp", tokenizedDs.col("sentiment").cast(DoubleType))
//      .drop("sentiment")
//      .withColumnRenamed("sentimentTmp", "sentiment")
//      .withColumn("wordsTmp", tokenizedDs.col("words").cast(DataTypes.createArrayType(StringType)))
//      .drop("words")
//      .withColumnRenamed("wordsTmp", "words")
//  }

  def parse(row: Row): Review = {
    val id = row.getAs[String]("id")
    val reviewText = row.getAs[String]("review")
    val sentiment = row.getAs[String]("sentiment").toDouble

    val tokenizedText = tokenizeText(reviewText)

    Review(id, tokenizedText, sentiment)
  }

  def tokenizeText(text: String): Seq[String] = {
    val analyzer = new EnglishAnalyzer()
    val tokenStream = analyzer.tokenStream("contents", text)

    val term = tokenStream.addAttribute(classOf[CharTermAttribute])
    tokenStream.reset()

    val tokenizedText = mutable.ArrayBuffer.empty[String]

    while (tokenStream.incrementToken()) {
      tokenizedText += term.toString
    }
    tokenStream.close()
    tokenStream.end()

    tokenizedText
  }
}

case class Review(id: String, words: Seq[String], sentiment: Double)
