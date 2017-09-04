package raw.inputformats.nifti

import raw.inputformats.InputFormat

/**
  * Created by dperez on 25.08.17.
  */

trait NiftiInputFormat extends InputFormat {
  final val formatId = "nifti"

  protected final val LITTLEENDIAN = "little_endian"
  protected final val DATATYPE = "datatype"
  protected final val BITPIX = "bitpix"
  protected final val NDIM = "nDim"
}

