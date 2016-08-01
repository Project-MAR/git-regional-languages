package gitlang

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn.{ readLine, readInt }
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ SQLContext, DataFrame }
import java.io.{ Console => _, _ }
import org.sameersingh.scalaplot.Implicits._

object Core extends App {

  val verbose = true

  // Initialise a local Spark context
  val conf = new SparkConf().setMaster("local").setAppName("vor")
  val sc = new SparkContext(conf)

  // Reads in the "dist.json" distribution data file
  val distjson = new File("src/main/resources/dist.json")
  val sqlctx = DistDataSource.readJSON(sc, distjson.getAbsolutePath())
  val dists = DistDataSource.getDistributionByLanguage(sqlctx, verbose)

  // Filter only those languages with distributions
  val dists_ = dists.filter("SIZE(coords)>0")

  // Analyse the distribution with histogram-based method
  // println(Console.MAGENTA + "******* HISTOGRAM ANALYSIS ********" + Console.RESET)
  // HistogramAnalysis.analyse(sc, sqlctx, dists_, verbose)

  // Analyse the distribution with 2D data
  println(Console.MAGENTA + "******* 2D ANALYSIS ********" + Console.RESET)
  TwoDimAnalysis.analyse(sc, sqlctx, dists_, verbose)
}

object TwoDimAnalysis {
  def analyse(sc: SparkContext, sqlctx: SQLContext, dists: DataFrame, verbose: Boolean) {

    // Create bin histograms
    val histograms = Transform.toHistograms(sc, sqlctx, dists)

    // Normalise bin histograms
    val ratioHistograms = Transform.toRatioBins(histograms)

    // Illustrate histograms
    if (verbose) {
      println(Console.CYAN + "***************** HISTOGRAMS ************" + Console.RESET)
      ratioHistograms foreach { (hist) =>
        println(hist.mkString(","))
        println("-------------------------------")
      }

      // Plot the histograms
      val toY = (in: Array[Double]) => Y(in, "bin")
      val x = (1 until ratioHistograms(0).size).map(_.toDouble)
      val ys = ratioHistograms.map(toY)
      output(GUI, xyChart(x -> ys))
    }
  }
}

object HistogramAnalysis {
  def analyse(sc: SparkContext, sqlctx: SQLContext, dists: DataFrame, verbose: Boolean) {
    // Accumulate the entire geospatial universe of the distributions
    val universe = Analysis.accumGlobalDists(sqlctx, dists)

    // Illustrate the universe distribution
    if (verbose) {
      println(Console.CYAN + s"Total geolocation spots : ${universe.keys.size}" + Console.RESET)
      for (xy <- universe) {
        println(xy)
      }
    }

    // Group geospatial distributions data of each language
    // into [bin] so we have fixed-length numerical vectors.
    val binVectors = Transform.geoDistToBins(sqlctx, dists, universe)

    // Classify the bin vectors into K different patterns
    val K = 6
    val (kmeans, gmm) = Analysis.learnPatterns(sc, K, binVectors, verbose)

    // Classify language distribution into K groups as learned
    val (clusterKMeans, clusterGMM) = Analysis.examineClusters(kmeans, gmm, binVectors, verbose)

    // Show the clustering results
    println(Console.MAGENTA + "*******************************" + Console.RESET)
    clusterKMeans.foreach {
      case (group, members) =>
        println(Console.MAGENTA + s"[KMeans Group: #${group}]" + Console.RESET)
        println(members.mkString(" , "))
    }
  }
}