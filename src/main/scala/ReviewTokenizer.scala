import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.Version
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, Tokenizer}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.io.StdIn
import scala.collection.mutable

/**
  * Tokenizes a review (cuts it up) and wraps it in a Review object
  */
object ReviewTokenizer {
  val LuceneVersion = Version.LATEST
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()
  import spark.implicits._

  def parseAll(rows: Dataset[Row]): Dataset[Row] = {

    val tokenizedDs = new Tokenizer()
    .setInputCol("review")
      .setOutputCol("words")
      .transform(rows).select("id", "words", "sentiment")

    tokenizedDs.withColumn("sentimentTmp", tokenizedDs.col("sentiment").cast(DoubleType))
      .drop("sentiment")
      .withColumnRenamed("sentimentTmp", "sentiment")
      .withColumn("wordsTmp", tokenizedDs.col("words").cast(DataTypes.createArrayType(StringType)))
      .drop("words")
      .withColumnRenamed("wordsTmp", "words")
  }

  def parse(row: Row): Row = {
    val id = row.getAs[String]("id")
    val reviewText = row.getAs[String]("review")
    val sentiment = extractSentiment(row)

    val tokenizedText = tokenizeText(reviewText)

    Row(id, tokenizedText, sentiment)
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

  def extractSentiment(row: Row): Double = {
    row.getAs[String]("sentiment").toDouble
  }
}

case class Review(id: String, words: Seq[String], sentiment: Double)
