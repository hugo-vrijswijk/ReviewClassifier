name := "Classifier"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // Spark for cluster computing and Naive Bayes machine learning lib
  "org.apache.spark" % "spark-core_2.11" % "2.1.+",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.+",
  // Lucene for the text analyzers/tokenizers
  "org.apache.lucene" % "lucene-core" % "6.3.+",
  "org.apache.lucene" % "lucene-analyzers-common" % "6.3.+"

  //  "com.github.fommil.netlib" % "all" % "1.1.2",
//  "org.scalanlp" %% "breeze" % "0.12",
//  "org.scalanlp" %% "breeze-natives" % "0.12",
//  "org.scalanlp" %% "breeze-viz" % "0.12"
)

