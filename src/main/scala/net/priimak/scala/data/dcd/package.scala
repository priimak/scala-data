package net.priimak.scala.data

import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

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
   * @param file file handle (RandomAccessFile) to opened file
   * @param frameSize size of each frame in bytes
   * @param  framesStartPos possition within file where first frame starts
   */
  case class DCD(
          byteOrder:      ByteOrder,
          scale:          Scale,
          frames:         Int, // number of sets of coordinates
          t0:             Int, // starting timestamp
          stepsInFrame:   Int, // the number of timesteps between dcd saves
          freeAtoms:      Int, // number of free atoms
          fixedAtoms:     Int, // number of fixed atoms
          dt:             Float,
          title:          String,
          natoms:         Int, // number of atoms reported in each frame
          freeIdx:        Array[Int],
          file:           RandomAccessFile,
          frameSize:      Long, // size of frame in bytes
          framesStartPos: Long // offset in file where first frame starts
      ) {

    // size of block in bytes that contains x, y or z coordinates within one frame
    private val coordBlockSize = 8 + 4 * freeAtoms
    private val zBlockOffset = 2 * coordBlockSize

    private val yBlockIndexOffset = 2 + freeAtoms
    private val zBlockIndexOffset = 2 * yBlockIndexOffset

    private val fileChannel = file.getChannel
    private val frameMaps = if (framesStartPos < 0) null else {
      for (frameId <- 0 until frames)
        yield {
          fileChannel.
            map(FileChannel.MapMode.READ_ONLY, framesStartPos + frameId * frameSize, frameSize).
            order(nativeByteOrder)
        }
    }

    /**
     * Close file channel, but not RandomAccessFile passed to the constructor
     */
    def close(): Unit = fileChannel.close()

    override def toString: String =
      s"DCD(\n     byteOrder = $byteOrder\n         scale = $scale\n         nsets = $frames\n            t0 = $t0" +
        s"\n  stepsInFrame = $stepsInFrame\n         atoms = $natoms\n     freeAtoms = $freeAtoms" +
        s"\n     fixedAtoms = $fixedAtoms\n            dt = ${48.88821 * dt}\n        title = $title" +
        s"\n    freeIdx.size = ${freeIdx.length}\n  frameSize = $frameSize\n)"

    /**
     * Get coordinates of each atom in a given frame
     *
     * @param index frame index
     */
    def frame(index: Int): IndexedSeq[Coord] = new IndexedSeq[Coord]() {
      private val frame = frameMaps(index)

      override def length: Int = freeAtoms

      override def apply(atomIndex: Int): Coord = {
        val x = frame.getFloat(atomIndex * 4 + 4) // readFloat(file, frameOffset)
        val y = frame.getFloat(atomIndex * 4 + 4 + coordBlockSize) //readFloat(file, coordBlockSize + frameOffset)
        val z = frame.getFloat(atomIndex * 4 + 4 + zBlockOffset) // readFloat(file, 2 * coordBlockSize + frameOffset)
        Coord(x, y, z)
      }
    }

    /**
     * Get time series of coordinates for a given atom identified by the atomIndex.
     *
     * @param atomIndex index of the atom for which to obtain
     * @return
     */
    def atom(atomIndex: Int): IndexedSeq[Coord] = new IndexedSeq[Coord]() {
      // offset to coordinate value within each coordinate block
      private val atomBytesOffset = framesStartPos + atomIndex * 4 + 4

      override def length: Int = frames

      override def apply(timeIndex: Int): Coord = frame(timeIndex)(atomIndex)
    }
  }

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
     * Operate on dcd object loaded from file.
     *
     */
    def withFile[A](file: String)(f: DCD => A): A = {
      val fh = new RandomAccessFile(file, "r")
      try {
        inFile(fh) { (_, dcd) =>
          try {
            f(dcd)
          } finally {
            dcd.close()
          }
        }
      } finally {
        fh.close()
      }
    }

    private def readHeader(rawHeader: ByteBuffer, fh: RandomAccessFile): DCD = {
      val (byteOrder, scale) = {
        val magicOne = rawHeader.getInt(0)
        val magicTwo = rawHeader.getInt(4)
        val cordName = Array(rawHeader.get(4), rawHeader.get(5), rawHeader.get(6), rawHeader.get(7))

        if (magicOne + magicTwo == 84)
          throw new Exception("64Bit DCD files are not supported")
        else if (magicOne == 84 && cordName.sameElements(cord))
          (nativeByteOrder, Scale32)
        else
          throw new Exception("Either unsupported byte order or not DCD file")
      }

      if (rawHeader.getInt(88) != 84)
        throw new Exception("Invalid header block ending")
      else
        DCD(
          byteOrder = byteOrder,
          scale = scale,
          frames = rawHeader.getInt(8),
          t0 = rawHeader.getInt(12),
          stepsInFrame = rawHeader.getInt(16),
          fixedAtoms = rawHeader.getInt(40),
          dt = rawHeader.getFloat(44),

          file = fh,
          natoms = -1,
          title = "",
          freeAtoms = -1,
          freeIdx = Array.empty,
          frameSize = -1,
          framesStartPos = -1
        )
    }

    private def readTitleAndIndex(file: RandomAccessFile, dcd: DCD): DCD = {
      val titleBlockSize = readInt(file, 92)
      if ((titleBlockSize - 4) % 80 != 0)
        throw new Exception(s"Invalid title block start $titleBlockSize")
      val ntitle = readInt(file, 96)
      val titleBytes = ntitle * 80
      val titleText = ByteBuffer.allocate(titleBytes).array()
      file.seek(100)
      file.readFully(titleText)

      val titleBlockEnd = readInt(file, 100 + titleBytes)

      if (readInt(file, 104 + titleBytes) != 4)
        throw new Exception("Invalid natoms block start")
      val natoms = readInt(file, 108 + titleBytes)
      if (readInt(file, 112 + titleBytes) != 4)
        throw new Exception("Invalid natoms block end")

      val totalFreeAtoms = natoms - dcd.fixedAtoms
      if (titleBlockSize != titleBlockEnd)
        throw new Exception("Invalid title block end")
      else
        readFreeIndex(file,
          (
            116 + titleBytes,
            dcd.copy(
              title = new String(titleText, StandardCharsets.US_ASCII),
              natoms = natoms,
              freeAtoms = totalFreeAtoms,
              frameSize = totalFreeAtoms * 12 + 24
            )
          )
        )
    }

    private def readFreeIndex(file: RandomAccessFile, src: (Long, DCD)): DCD = src match {
      case (pos, dcd) =>
        if (dcd.fixedAtoms == 0)
          src._2.copy(framesStartPos = src._1)
        else {
          // some atoms are fixed, which means that there is index for free atoms within corresponding pdb file
          // and we need to read this index
          if (readInt(file, pos) != dcd.freeAtoms + 4)
            throw new Exception("Invalid free index block start")

          val freeIndex = Array.newBuilder[Int]
          val startOfIndex = pos + 4
          (1 until dcd.freeAtoms).foreach(offset => {
            freeIndex += readInt(file, startOfIndex + offset)
          })

          if (readInt(file, startOfIndex + dcd.freeAtoms * 4) != dcd.freeAtoms * 4)
            throw new Exception("Invalid free index block start")

          dcd.copy(
            freeIdx = freeIndex.result(),
            framesStartPos = 4 + startOfIndex + dcd.freeAtoms * 4
          )
        }
    }

    /**
     * Fix malformed dcd files generated by the NAMD. At the moment this involves writing correct number of
     * frames (variable nset in the specification) into appropriate position in the header of the file. NAMD might
     * just write 0 or some other bogus value in that position.

     * @param file path to dcd file
     */
    def fixDCDFile(file: String): Unit = {
      val fh = new RandomAccessFile(file, "rw")
      try {
        inFile(fh) { (fh, dcd) =>
          val buffer = ByteBuffer.allocate(4).order(nativeByteOrder)
          val frames = ((fh.length() - dcd.framesStartPos) / dcd.frameSize).toInt
          buffer.putInt(frames)
          fh.seek(8)
          fh.write(buffer.array())
        }
      } finally {
        fh.close()
      }
    }

    /**
     * Given file handle (i.e. RandomAccessFile) read dcd header and title and index and then evaluate
     * function f(RandomAccessFile, DCD) on file handle and dcd object returning whatever function f(...) returns.
     */
    private def inFile[A](fh: RandomAccessFile)(f: (RandomAccessFile, DCD) => A): A = {
      val rawHeader = ByteBuffer.allocate(92)
      rawHeader.order(ByteOrder.nativeOrder())
      if (fh.length() < 92)
        throw new Exception("Not a dcd file")
      else {
        fh.readFully(rawHeader.array())
        f(fh, readTitleAndIndex(fh, readHeader(rawHeader, fh)))
      }
    }
  }

  private val nativeByteOrder = ByteOrder.nativeOrder()

  private def readFloat(file: RandomAccessFile, position: Long): Float = {
    file.seek(position)
    val value = ByteBuffer.allocate(4).order(nativeByteOrder)
    file.readFully(value.array())
    value.getFloat
  }

  private def readInt(file: RandomAccessFile, position: Long): Int = {
    file.seek(position)
    val value = ByteBuffer.allocate(4).order(nativeByteOrder)
    file.readFully(value.array())
    value.getInt
  }

  private def readByte(file: RandomAccessFile, position: Long): Byte = {
    file.seek(position)
    file.read().toByte
  }
}
