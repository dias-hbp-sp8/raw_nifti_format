package raw.inputformats.nifti

/**
  * Created by dperez on 25.08.17.
  */
import raw.inputformats.InputFormatHint

import scala.collection.mutable
import scala.util.Try

/**
  * Created by dperez on 23.06.17.
  */
object NiftiInputFormatHint extends NiftiInputFormat {

  def apply(little_endian: Option[Boolean] = None,
            datatype: Option[Int] = None,
            bitpix: Option[Int] = None,
            nDim: Option[Int] = None): InputFormatHint = {
    val hints = new mutable.HashMap[String, String]()
    little_endian.foreach(v => hints.put(LITTLEENDIAN, little_endian.toString))
    datatype.foreach(flag => hints.put(DATATYPE, datatype.toString))
    bitpix.foreach(flag => hints.put(BITPIX, bitpix.toString))
    nDim.foreach(flag => hints.put(NDIM, nDim.toString))
    new InputFormatHint(formatId, hints.toMap)
  }

  def unapply(format: InputFormatHint): Option[(Option[Boolean], Option[Int], Option[Int], Option[Int])] = {
    if (format.formatId != formatId) {
      None
    } else {
      Some(
        format.hints.get(LITTLEENDIAN).map(le => Try(le.toBoolean).getOrElse(false)),
        format.hints.get(DATATYPE).map(java.lang.Integer.parseInt),
        format.hints.get(BITPIX).map(java.lang.Integer.parseInt),
        format.hints.get(NDIM).map(java.lang.Integer.parseInt)
      )
    }
  }

}