package net.priimak.scala.data

import java.io._
import java.nio._
import java.nio.file._

/**
 * Package of classes and methods that deal with dcd trajectory files.
 */
package object dcd {
  /**
   * Content of the DCD file
   *
   * @param byteOrder little or big endian
   * @param scale 32 or 64 bit
   * @param frames number of sets/frames within file
   * @param t0 starting timestamp
   * @param stepsInFrame number of steps beteen each frame
   * @param fixedAtoms number of fixed atoms
   * @param dt time interval
   * @param title title (comment) present in the dcd file
   * @param natoms total number of atoms
   * @param freeIdx index of free atoms. If empty then all atoms are free
   * @param rawFrames raw data containing frames
   */
  case class DCD(
        byteOrder:    ByteOrder,
        scale:        Scale,
        frames:        Int, // number of sets of coordinates
        t0:           Int, // starting timestamp
        stepsInFrame: Int, // the number of timesteps between dcd saves
        freeAtoms:    Int, // number of free atoms
        fixedAtoms:   Int, // number of fixed atoms
        dt:           Float,
        title:        String,
        natoms:       Int,
        freeIdx:      Array[Int],
        rawFrames:    ByteBuffer,
        frameBytes:   Int,
        framesStartPos: Int
      ) {
    // size of block in bytes that contains x, y or z coordinates within one frame
    private val coordBlockSize = 8 + 4 * freeAtoms

    override def toString: String =
      s"DCD(\n     byteOrder = $byteOrder\n         scale = $scale\n         nsets = $frames\n            t0 = $t0" +
        s"\n  stepsInFrame = $stepsInFrame\n         atoms = $natoms\n     freeAtoms = $freeAtoms" +
        s"\n     fixedAtoms = $fixedAtoms\n            dt = ${48.88821 * dt}\n        title = $title" +
        s"\n    freeIdx.size = ${freeIdx.length}\n  frameBytes=$frameBytes\n)"

    /**
     * Get time series of coordinates for a given atom identified by the atomIndex.
     *
     * @param atomIndex index of the atom for which to obtain
     * @return
     */
    def atom(atomIndex: Int): IndexedSeq[Coord] = new IndexedSeq[Coord]() {
      // offset to coordinate value within each coordinate block
      private val atomBytesOffset = framesStartPos + atomIndex * 4

      override def length: Int = frames

      override def apply(timeIndex: Int): Coord = {
        val frameOffset = timeIndex * frameBytes
        Coord(
          rawFrames.getFloat(4 + frameOffset + atomBytesOffset),
          rawFrames.getFloat(4 + coordBlockSize + frameOffset + atomBytesOffset),
          rawFrames.getFloat(4 + 2 * coordBlockSize + frameOffset + atomBytesOffset)
        )
      }
    }
  }

  /**
   * Coordinates in the 3d space
   */
  final case class Coord(x: Float, y: Float, z: Float)

  /**
   * Scale of dcd file. Either Scale32 (contains floats for coordinates) or Scale64 (contains doubles for coordinates).
   */
  sealed abstract class Scale
  case object Scale32 extends Scale
  case object Scale64 extends Scale

  object DCD {
    // part of the magic header
    private val cord = "CORD".toCharArray.map(_.toByte)

    /**
     * Read DCD object from .dcd file.
     *
     * @param file file name of the .dcd file
     * @return instance of DCD
     */
    def valueOf(file: String): DCD = {
      val rawData = ByteBuffer.wrap(Files.readAllBytes(Paths.get(file)))

      // first 8 bytes contains magic header that can also be used to detect endianes
      def readMagic(byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): (ByteOrder, Scale) = {
        rawData.rewind()
        rawData.order(byteOrder)
        val magicOne = rawData.getInt()
        val magicTwo = rawData.getInt()
        rawData.position(4)
        val cordName = Array(rawData.get(), rawData.get(), rawData.get(), rawData.get())

        if (magicOne + magicTwo == 84)
          (rawData.order(), Scale64)
        else if (magicOne == 84 && cordName.sameElements(cord))
          (rawData.order(), Scale32)
        else if (byteOrder == ByteOrder.LITTLE_ENDIAN)
          throw new IOException("Unable to recognise DCD file from its header")
        else
          readMagic(ByteOrder.LITTLE_ENDIAN)
      }

      val (byteOrder, scale) = readMagic()
      if (scale == Scale64)
        throw new Exception("64-bit dcd files are unsupported")

      // set byte order to what we have detected so far
      rawData.order(byteOrder)

      if (rawData.getInt(88) != 84)
        throw new Exception("Invalid header block ending")

      val titleBlockSize = rawData.getInt(92)
      if ((titleBlockSize - 4) % 80 != 0)
        throw new Exception("Invalid title block start")

      val ntitle = rawData.getInt(96)
      rawData.position(100)

      val comment = StringBuilder.newBuilder
      val titleBytes = ntitle * 80
      (1 to titleBytes).foreach(_ => comment.append(rawData.get().toChar))

      val titleBlockEnd = rawData.getInt(100 + titleBytes)
      if (titleBlockSize != titleBlockEnd)
        throw new Exception("Invalid title block end")

      if (rawData.getInt(104 + titleBytes) != 4)
        throw new Exception("Invalid natoms block start")

      if (rawData.getInt(112 + titleBytes) != 4)
        throw new Exception("Invalid natoms block end")

      val totalFixedAtoms = rawData.getInt(40)
      val natoms = rawData.getInt(108 + titleBytes)
      val totalFreeAtoms = natoms - totalFixedAtoms

      rawData.position(116 + titleBytes)
      val freeIndex = Array.newBuilder[Int]
      if (totalFixedAtoms > 0) {
        // some atoms are fixed, which means that there is index for free atoms within corresponding pdb file
        // and we nned to read this index
        if (rawData.getInt() != totalFreeAtoms * 4)
          throw new Exception("Invalid free index block start")

        (1 to totalFreeAtoms).foreach(_ => freeIndex += rawData.getInt)

        if (rawData.getInt() != totalFreeAtoms * 4)
          throw new Exception("Invalid free index block start")
      }

      val fStart = rawData.position()

      DCD(
        byteOrder = byteOrder,
        scale = scale,
        frames = rawData.getInt(8),
        t0 = rawData.getInt(12),
        stepsInFrame = rawData.getInt(16),
        freeAtoms = totalFreeAtoms,
        fixedAtoms = totalFixedAtoms,
        dt = rawData.getFloat(44),
        title = comment.toString(),
        natoms = rawData.getInt(108 + titleBytes),
        freeIdx = freeIndex.result(),
        rawFrames = rawData,
        frameBytes = totalFreeAtoms * 12 + 24,
        framesStartPos = fStart
      )
    }
  }
}

