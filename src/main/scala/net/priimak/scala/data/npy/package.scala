package net.priimak.scala.data

import net.priimak.scala.data.npy._

package object npy {
  import java.nio._
  import java.nio.charset._
  import java.nio.file._

  abstract class NPYVector[A](len: Int, valueType: String) extends IndexedSeq[A] {
    override def length: Int = len
    override def toString(): String =
      "NPYVector[%s](%s...)".format(valueType, take(7).map(_ + ", ").reduce((x,y) => x + y))
    def npyType: String = valueType
  }

  private final case class UByteVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Int](len, valueType) {
    override def apply(idx: Int): Int = java.lang.Byte.toUnsignedInt(rawData.get(offset + idx))
  }

  private final case class ByteVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Byte](len, valueType) {
    override def apply(idx: Int): Byte = rawData.get(offset + idx)
  }

  private final case class UShortVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Int](len, valueType) {
    override def apply(idx: Int): Int = java.lang.Short.toUnsignedInt(rawData.getShort(offset + 2 * idx))
  }

  private final case class ShortVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Short](len, valueType) {
    override def apply(idx: Int): Short = rawData.getShort(offset + 2 * idx)
  }

  private final case class UIntVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Long](len, valueType) {
    override def apply(idx: Int): Long = Integer.toUnsignedLong(rawData.getInt(offset + 4 * idx))
  }

  private final case class IntVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Int](len, valueType) {
    override def apply(idx: Int): Int = rawData.getInt(offset + 4 * idx)
  }

  private final case class ULongVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Long](len, valueType) {
    override def apply(idx: Int): Long = rawData.getLong(offset + 8 * idx)
  }

  private final case class LongVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Long](len, valueType) {
    override def apply(idx: Int): Long = rawData.getLong(offset + 8 * idx)
  }

  private final case class FloatVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Float](len, valueType) {
    override def apply(idx: Int): Float = rawData.getFloat(offset + 4 * idx)
  }

  private final case class DoubleVector(rawData: ByteBuffer, offset: Int, len: Int, valueType: String)
      extends NPYVector[Double](len, valueType) {
    override def apply(idx: Int): Double = rawData.getDouble(offset + 8 * idx)
  }

  private val npyHeader = raw"^\{'descr': '(.+)', 'fortran_order': (.+), 'shape': \((.*)\), \}".r
  private val npyDescrHeader = "^[<>|=]?(.+)$".r

  object NPYVector {
    /**
     * Load 1-dimensional vectro from .npy file
     */
    def valueOf[A](file: String): NPYVector[A] = {
      val rawData = ByteBuffer.wrap(Files.readAllBytes(Paths.get(file)))
      val magic = new Array[Byte](6)
      rawData.get(magic)
      if (!(magic sameElements Array(-109, 0x4e, 0x55, 0x4d, 0x50, 0x59)))
        throw new IllegalArgumentException("Invalid npy file magic header")

      val majorVersion = rawData.get
      val minorVersion = rawData.get
      if (majorVersion != 0x1 || minorVersion != 0x0)
        throw new IllegalArgumentException(s"Unsupported npy file version $majorVersion.$minorVersion")

      rawData.order(ByteOrder.LITTLE_ENDIAN)
      val headerLength = java.lang.Short.toUnsignedInt(rawData.getShort)

      val header = new Array[Byte](headerLength)
      rawData.get(header)
      val npyHeader(description, fortranOrder, shape) = new String(header, StandardCharsets.UTF_8).trim

      if (fortranOrder == "True")
        throw new IllegalArgumentException("Fortran order in the the npy file is unsupported")

      val byteOrder = if (description.head == '<') ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
      val npyDescrHeader(valueType) = description

      val dimentions = shape.split(",").map(_.trim).filterNot(_.isEmpty).map(_.toInt)
      if (dimentions.length != 1)
        throw new IllegalArgumentException(
          "Currently only one dimensional arrays are supported, while file contains %s dimentional array".
            format(dimentions.length)
        )
      rawData.order(byteOrder)
      rawData.position()

      val vectorCons = valueType match {
        case "u1" => UByteVector
        case "i1" => ByteVector
        case "u2" => UShortVector
        case "i2" => ShortVector
        case "u4" => UIntVector
        case "i4" => IntVector
        case "u8" => ULongVector
        case "i8" => LongVector
        case "f4" => FloatVector
        case "f8" => DoubleVector
        case _ => throw new IllegalArgumentException(s"Unrecognized array element type $valueType")
      }
      vectorCons(rawData, rawData.position(), dimentions.head, valueType).asInstanceOf[NPYVector[A]]
    }
  }
}
