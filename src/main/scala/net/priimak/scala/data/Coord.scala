package net.priimak.scala.data

/**
 * Coordinates in the 3D space.
 */
final case class Coord(x: Double, y: Double, z: Double) {
  def +(that: Coord): Coord = Coord(x + that.x, y + that.y, z + that.z)

  def -(that: Coord): Coord = Coord(x - that.x, y - that.y, z - that.z)

  def *(c: Double): Coord = Coord(x * c, y * c, z * c)

  def /(c: Double): Coord = Coord(x / c, y / c, z / c)

  def length: Double = math.sqrt(x * x + y * y + z * z)

  /**
   * x**2 + y**2 + z**2
   */
  def r2: Double = x * x + y * y + z * z

  /**
   * Normalize coordinates on the periodic boundary conditions (PDB)
   *
   * @param xBox interval defining PDB along the x-axis
   * @param yBox interval defining PDB along the y-axis
   * @param zBox interval defining PDB along the z-axis
   * @return instance of Coord with coordinates placed withing the box of (xBox, yBox, zBox)
   */
  def normilizeOnPDB(xBox: Interval, yBox: Interval, zBox: Interval): Coord =
    Coord(normilizeInBox(x, xBox), normilizeInBox(y, yBox), normilizeInBox(z, zBox))

  private def normilizeInBox(x: Double, box: Interval): Double =
    ((x - box.minVal) % box.width + box.width) % box.width + box.minVal
}
