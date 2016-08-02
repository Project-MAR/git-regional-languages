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

    // Illustrate histograms
    if (verbose) {
      println(Console.CYAN + "***************** HISTOGRAMS ************" + Console.RESET)
      ratioHistograms foreach { (hist) =>
        println(hist.mkString(","))
        println("-------------------------------")
      }

      // Plot the fractional-ratio histograms
      val toY = (in: Array[Double]) => Y(in, style = XYPlotStyle.Lines)
      val x = (0 until ratioHistograms(0).size).map(_.toDouble)
      val ys = ratioHistograms.map(toY).toSeq

      output(GUI, xyChart(x -> ys))
    }

    // TAOTODO:
  }
}
