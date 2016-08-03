package gitlang

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }
import scala.collection.mutable.{ Map, WrappedArray }
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.clustering.{ GaussianMixture, GaussianMixtureModel }
import org.apache.spark.mllib.linalg.{ Vectors, Matrices }

/**
 * Spatial distribution recognition object
 */
object Patterns {

  def createCluster(sc: SparkContext, vectors: Array[LanguageHistogram], k: Int, nIters: Int): KMeansModel = {

    // Make RDD from input array
    val rdd = sc.parallelize(vectors.map { v =>
      Vectors.dense(v.binVector)
    } toSeq)

    // Cluster the RDD vectors
    val clusters = KMeans.train(rdd, k, nIters)
    clusters
  }

  def cluster(sc: SparkContext, vectors: Array[LanguageHistogram], model: KMeansModel): Array[(Int, LanguageHistogram)] = {

    // Make RDD from the input array
    vectors map {
      case LanguageHistogram(lang, v) =>
        val vector = Vectors.dense(v)
        val cluster = model.predict(vector)
        (cluster, LanguageHistogram(lang, v))
    }
  }
}