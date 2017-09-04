package raw.inputformats.nifti

import raw.inputformats.{InputFormatDescriptor}

/**
  * Created by dperez on 25.08.17.
  */
object NiftiInputFormatDescriptor extends NiftiInputFormat {
  def apply(littleEndian: Boolean,
            datatype: Int,
            bitpix: Int,
            nDim: Int): InputFormatDescriptor = {
    InputFormatDescriptor(
      formatId,
      Map(
        LITTLEENDIAN -> littleEndian.toString,
        DATATYPE -> datatype.toString,
        BITPIX -> bitpix.toString,
        NDIM -> nDim.toString
      ))
  }

  def unapply(format: InputFormatDescriptor): Option[(Boolean, Int, Int, Int)] = {
    if (format.formatId != formatId) {
      None
    } else {
      Some(
        java.lang.Boolean.parseBoolean(format.options(LITTLEENDIAN)),
        format.options(DATATYPE).toInt,
        format.options(BITPIX).toInt,
        format.options(NDIM).toInt
      )
    }
  }

}
