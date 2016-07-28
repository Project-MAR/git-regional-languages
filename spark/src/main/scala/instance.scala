package gitlang

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn.{ readLine, readInt }
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ SQLContext, DataFrame }
import java.io.{ Console => _, _ }

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

  HistogramAnalysis.analyse(sc, sqlctx, dists_, verbose)
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

    // Visualise the resultant bin vectors
    //// Plot.plotBinVectors(binVectors)

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