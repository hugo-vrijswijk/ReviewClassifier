name := "Classifier"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "net.tixxit" %% "delimited-core" % "0.8.0",
  "org.apache.spark" % "spark-core-2.11" % "2.0.2",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.2"
//  "com.github.fommil.netlib" % "all" % "1.1.2",
//  "org.scalanlp" %% "breeze" % "0.12",
//  "org.scalanlp" %% "breeze-natives" % "0.12",
//  "org.scalanlp" %% "breeze-viz" % "0.12"
)

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"



