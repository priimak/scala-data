package net.priimak.scala.data

import java.io.{File, PrintWriter}

import net.priimak.scala.data.dcd._

import scala.io._
import math.sqrt

/**
 * Classes and methods that deal with molecular structures.
 */
package object ms {
  /**
   * Wrapper class for atom name
   */
  final case class Atom(name: String)

  /**
   * Extended atom which encapsulates information about atom within molecular structure
   *
   * @param atom atom name
   * @param coordinates 3d coordinates, i.e. a position of atom within the structure
   * @param occupancy occupancy factor
   * @param tempf temperature factor
   * @param rt position of this atom as a time series
   */
  final case class XAtom(
      serial: Int,
      atom: Atom,
      residue: String,
      resSeqNum: Int,
      coordinates: Coord,
      occupancy: Float,
      tempf: Float,
      sym: String,
      rt: IndexedSeq[Coord]) {
    def x: Double = coordinates.x
    def y: Double = coordinates.y
    def z: Double = coordinates.z
  }

  /**
   * Class identifying molecular structure.
   */
  case class MS(name: String) extends IndexedSeq[XAtom] {
    /**
     * List of residue sub-sequences.
     */
    def residues: IndexedSeq[MS] = IndexedSeq.empty

    override def toString: String = s"MS($name)"

    override def length: Int = 0

    /**
     * Create molecular structure extended with time series from the DCD object/file.
     *
     * @param dcd dcd time series
     * @return instance of MS extended with time series from the DCD object/file
     */
    def withDCD(dcd: DCD): MS = {
      lazy val atoms = this.map(a => a.copy(rt = {
        new IndexedSeq[Coord] {
          override def length: Int = dcd.frames
          override def apply(idx: Int): Coord = dcd.atom(a.serial)(idx)
        }
      }))

      val rsd = this.residues

      new MS(name) {
        override def residues: IndexedSeq[MS] = rsd
        override def length: Int = atoms.length
        override def apply(idx: Int): XAtom = atoms(idx)
      }
    }

    override def apply(idx: Int): XAtom = null
  }

  object MS {
    private val ATOMLR =
      """^ATOM  ([\s\d]{5}).(.{4}).(.{3}).(.)([\s\d]{4})....([-\s\d\.]{8})(
        |[-\s\d\.]{8})([-\s\d\.]{8})([-\s\d\.]{6})([-\s\d\.]{6})......(.{4})(..).*$""".r
    private val ATOMHR =
      """^ATOM  ([\s\dabcdefABCDEF]{5}).(.{4}).(.{3}).(.)([\s\d]{4})....([-\s\d\.]{8})(
        |[-\s\d\.]{8})([-\s\d\.]{8})([-\s\d\.]{6})([-\s\d\.]{6})......(.{4})(..).*$""".r
    private val ATOMCR =
      """^ATOM  \*{5}.(.{4}).(.{3}).(.)([\s\d]{4})....([-\s\d\.]{8})([-\s\d\.]{8})(
        |[-\s\d\.]{8})([-\s\d\.]{6})([-\s\d\.]{6})......(.{4})(..).*$""".r

    /**
     * Load molecular structure from PDB file.
     *
     * @param file pdb file name
     * @return instance of molecular structure
     */
    def fromPDBFile(file: String): MS = {
      var lastSerialNumber = 0
      val atoms = Source.fromFile(file).getLines().flatMap(line => {
        line match {
          case ATOMLR(serial, atomName, resName, chainId, resSeqNum, x, y, z, occupancy, tempf, segmentId, eSym) =>
            lastSerialNumber = serial.trim.toInt
            Some(XAtom(
              lastSerialNumber,
              Atom(atomName.trim),
              resName.trim,
              resSeqNum.trim.toInt,
              Coord(x.trim.toFloat, y.trim.toFloat, z.trim.toFloat),
              occupancy.trim.toFloat,
              tempf.trim.toFloat,
              eSym.trim,
              IndexedSeq.empty
            ))
          case ATOMHR(serial, atomName, resName, chainId, resSeqNum, x, y, z, occupancy, tempf, segmentId, eSym) =>
            lastSerialNumber = Integer.parseInt(serial.trim, 16)
            Some(XAtom(
              lastSerialNumber,
              Atom(atomName.trim),
              resName.trim,
              resSeqNum.trim.toInt,
              Coord(x.trim.toFloat, y.trim.toFloat, z.trim.toFloat),
              occupancy.trim.toFloat,
              tempf.trim.toFloat,
              eSym.trim,
              IndexedSeq.empty
            ))
          case ATOMCR(atomName, resName, chainId, resSeqNum, x, y, z, occupancy, tempf, segmentId, eSym) =>
            lastSerialNumber = lastSerialNumber + 1
            Some(XAtom(
              lastSerialNumber,
              Atom(atomName.trim),
              resName.trim,
              resSeqNum.trim.toInt,
              Coord(x.trim.toFloat, y.trim.toFloat, z.trim.toFloat),
              occupancy.trim.toFloat,
              tempf.trim.toFloat,
              eSym.trim,
              IndexedSeq.empty
            ))
          case _ => None
        }
      }).toIndexedSeq

      new MS("root") {
        override def length: Int = atoms.length
        override def apply(idx: Int): XAtom = atoms(idx)
      }
    }
  }
}
