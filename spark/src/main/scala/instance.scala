package gitlang

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn.{ readLine, readInt }
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ SQLContext, DataFrame }
import java.io.{ Console => _, _ }
import org.sameersingh.scalaplot.Implicits._
import org.sameersingh.scalaplot._

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

  // Analyse the distribution with 2D histogram data
  println(Console.MAGENTA + "******* 2D ANALYSIS ********" + Console.RESET)
  TwoDimAnalysis.analyse(sc, sqlctx, dists_, verbose)
}

object TwoDimAnalysis {
  def analyse(sc: SparkContext, sqlctx: SQLContext, dists: DataFrame, verbose: Boolean) {

    // Create bin histograms
    val histograms = Transform.toHistograms(
      sc, sqlctx, dists,
      20, 20
    )

    // Normalise bin histograms
    val ratioHistograms = Transform.toRatioBins(histograms)

    // Filter histograms where the output vector
    // produces only up to 92% of the original vector
    val reducedHistograms = Transform.toReducedBins(
      ratioHistograms, 0.92
    )

    // Illustrate histograms
    if (verbose) {
      println(Console.CYAN + "***************** HISTOGRAMS ************" + Console.RESET)
      reducedHistograms foreach { (hist) =>
        println(s"[${hist.lang}]")
        println(hist.binVector.mkString(","))
        println("-------------------------------")
      }

      // Plot the fractional-ratio histograms
      val toY = (in: LanguageHistogram) => Y(in.binVector, style = XYPlotStyle.Lines)
      val x = (0 until reducedHistograms(0).size).map(_.toDouble)
      val ys = reducedHistograms.map(toY).toSeq

      output(GUI, xyChart(x -> ys, "All histograms"))

      // Also produce a PNG output as a physical file
      val escapeLang = (lang: String) => lang.replace("/", ",")
      val dir = new java.io.File(".").getCanonicalPath + "/plots/"
      reducedHistograms.foreach {
        case LanguageHistogram(lang, vecH) =>
          val yh = toY(LanguageHistogram(lang, vecH))
          val lang_ = escapeLang(lang)
          output(PNG(dir, lang_), xyChart(x -> Seq(yh), lang_))

          println(Console.GREEN + s"Saving ${lang}.gpl to ${dir}" + Console.RESET)
      }
    }

    // Learn the histogram patterns
    val (k, nIters) = (5, 10)
    val clusters = Patterns.createCluster(sc, reducedHistograms, 5, 10)

    // Visualise each of the clusters
    if (verbose) {
      val x = (0 until reducedHistograms(0).size).map(_.toDouble)
      val centroids = clusters
        .clusterCenters
        .map(n => Y(n.toArray))
        .toSeq
      println(Console.CYAN + "Visualising KMeans..." + Console.RESET)
      output(GUI, xyChart(x -> centroids, "KMeans Centroids"))
    }

    // Determine the cluster members
    val clusterList = Patterns.cluster(sc, reducedHistograms, clusters)
    val groups = clusterList.groupBy { case (cluster, hist) => cluster }
    groups.foreach {
      case (cluster, members) =>
        println(Console.MAGENTA + s"======= CLUSTER [${cluster}] =======" + Console.RESET)
        members.foreach {
          case (_, LanguageHistogram(lang, v)) =>
            println(lang)
        }

        // TAOTODO: Copy the distribution images of languages
        // in the same group into group directories
    }
  }
}
