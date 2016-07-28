package gitlang

case class SpatialLocation(lat: Double, lng: Double)

case class SpatialCodeSpot(pos: SpatialLocation, density: Double)
