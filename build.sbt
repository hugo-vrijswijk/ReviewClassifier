name := "Classifier"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // Spark for cluster computing and Naive Bayes machine learning lib
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  // Lucene for the text analyzers/tokenizers
  "org.apache.lucene" % "lucene-core" % "6.4.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "6.4.0",
  // Guava for the dictionary
  "com.google.guava" % "guava" % "21.0"

  //  "com.github.fommil.netlib" % "all" % "1.1.2",
//  "org.scalanlp" %% "breeze" % "0.12",
//  "org.scalanlp" %% "breeze-natives" % "0.12",
//  "org.scalanlp" %% "breeze-viz" % "0.12"
)

