package gitlang

case class SpatialHistogram(binVector: Array[Double]) {

  def toLangHistogram(lang: String) = LanguageHistogram(lang, binVector)

  def size = binVector.size
}

case class LanguageHistogram(lang: String, binVector: Array[Double]) {

  def toSpatialHistogram() = SpatialHistogram(binVector)

  def size = binVector.size
}