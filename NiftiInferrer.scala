package raw.inputformats.nifti

//import java.util.zip.GZIPInputStream

import raw.PublicException
import raw.compiler.base.source._
import raw.config.RawSettings
import raw.inputformats.InputFormatDescriptor
import raw.location.LocationManager
//import raw.inputformats.nifti.{NiftiInputFormatDescriptor, NiftiInputFormatHint}
import raw.location.{Location}
import raw.usermng.client.AuthenticatedClient

import scala.util.{Failure, Success, Try}

/**
  * Created by dperez on 15.03.17.
  */

object NiftiInferrer {

  def infer(locationManager: LocationManager, client: AuthenticatedClient, settings: RawSettings, location: Location): (Type, InputFormatDescriptor) = {

    // Open InputStream on file (.gz files are expected to be gun zipped)
    var temp_is = locationManager.getInputStream(location)
    val is = /*if (location.rawUri.endsWith(".gz")) new GZIPInputStream(temp_is) else*/ temp_is


    try {
      val header = NiftiHeader.read(is,"No valid filename here")

      val le = header.little_endian
      val dim0: Int = header.dim(0)
      val datatypeCode: Int = header.datatype
      val bitpix: Int = header.bitpix

      assert(dim0 < 8,s"NiftiInferrer: dim0 > 8 ($dim0)")
      assert(header.sizeof_hdr == 348, s"NiftiInferrer: header.sizeof_hdr != 348 (${header.sizeof_hdr})")
      assert(header.magic == "ni1" || header.magic == "n+1", s"NiftiInferrer: header.magic != ni1 or n+1 (${header.magic})")

      val datatype: Type = datatypeCode match {
        case NiftiHeader.DT_BINARY =>  FloatType()//BoolType()
        case NiftiHeader.NIFTI_TYPE_INT8 | NiftiHeader.NIFTI_TYPE_UINT8 | NiftiHeader.NIFTI_TYPE_INT16 | NiftiHeader.NIFTI_TYPE_UINT16 | NiftiHeader.NIFTI_TYPE_INT32 =>
          FloatType()//IntType()
        case NiftiHeader.NIFTI_TYPE_UINT32 | NiftiHeader.NIFTI_TYPE_INT64 =>  FloatType()//LongType()
        case NiftiHeader.NIFTI_TYPE_FLOAT32 => FloatType()
        case NiftiHeader.NIFTI_TYPE_COMPLEX64 => CollectionType(FloatType())
        case NiftiHeader.NIFTI_TYPE_RGB24 => CollectionType(IntType())

        case NiftiHeader.NIFTI_TYPE_UINT64 => throw new PublicException("Unsupported datatype: NIFTI_TYPE_UINT64")
        case NiftiHeader.NIFTI_TYPE_FLOAT64 => throw new PublicException("Unsupported datatype: NIFTI_TYPE_FLOAT64")
        case NiftiHeader.NIFTI_TYPE_FLOAT128 => throw new PublicException("Unsupported datatype: NIFTI_TYPE_FLOAT128")
        case NiftiHeader.NIFTI_TYPE_COMPLEX128 => throw new PublicException("Unsupported datatype: NIFTI_TYPE_COMPLEX128")  //CollectionType(???)
        case NiftiHeader.NIFTI_TYPE_COMPLEX256 => throw new PublicException("Unsupported datatype: NIFTI_TYPE_COMPLEX256")  //CollectionType(???)
        case _ => throw new PublicException(s"Unsupported datatype: $datatypeCode")
      }
      println("datatypeCode="+datatypeCode+", datatype="+datatype+", bitpix="+bitpix+", dim0="+dim0)

      // Use float array in all cases, as scaling will be applied to raw data and also for functions compatibility

      def getRightCollectionDim(tipe: Type, curDim: Int): Type =
        if (curDim < dim0) getRightCollectionDim(CollectionType(tipe), curDim + 1) else tipe
      val dataType = getRightCollectionDim(datatype, 0)

      val tipe = CollectionType(RecordType(Vector(AttrType("header", niftiHeaderType), AttrType("data", dataType))))
      val format = NiftiInputFormatDescriptor(
        littleEndian = le,
        datatype = datatypeCode,
        bitpix = bitpix,
        nDim = dim0
      )

      (tipe, format)
    }
    catch {
      case e: java.lang.AssertionError =>
        throw new PublicException("Failed NIFTI inferrence: "+e.getMessage)
      case e: Throwable =>
        throw new PublicException("Failed NIFTI inferrence: "+e.getMessage)
    }
    finally is.close()
  }

  // The niftiHeaderType defined here with RAW Types must match
  // the NiftiHeader class defined in NiftiHeader.scala,
  // so that when a NiftiHeader is read by NiftiHeader and returned,
  // it can be interpreted by RAW as a the niftiHeaderType expected.
  // This semi-duplication of code is not ideal but it is the simplest for now.
  //
  // Alternatively we could infer the niftiHeaderType from the NiftiHeader class.
  // I.e. reverse what "TypesBuilder" does right now.
  // However, the RAW Types impose limitations on the types used in NiftiHeader,
  // so there will remain constraints coming from both sides.

  val niftiHeaderType: RecordType = {
    RecordType(Vector[AttrType](

      // Extracted or supplementary fields

      AttrType("filename", StringType()),             // not strictly part of header
      AttrType("little_endian", BoolType()),         // not strictly part of header

      AttrType("freq_dim", IntType()),                  // obtained from dim_info
      AttrType("phase_dim", IntType()),                 // -> freq_dim, phase_dim, slice_dim:
      AttrType("slice_dim", IntType()),                 // integers (1, 2 or 3)

      AttrType("xyz_unit_code", IntType()),             // obtained from xyzt_units
      AttrType("t_unit_code", IntType()),               // -> unit text codes

      AttrType("qfac", IntType()),                      // -1 or 1, stored in the otherwise unused pixdim[0]

      AttrType("extensions_list", CollectionType(CollectionType(IntType()))),  // How to give access
      AttrType("extension_blobs", CollectionType(CollectionType(IntType()))), // to header extensions ?

      // Original header fields

      AttrType("sizeof_hdr", IntType()),                // MUST be 348

      AttrType("data_type_string", StringType()),       // UNUSED
      AttrType("db_name", StringType()),                // UNUSED
      AttrType("extents", IntType()),                   // UNUSED
      AttrType("session_error", IntType()),             // UNUSED
      AttrType("regular", StringType()),                // UNUSED
      AttrType("dim_info", StringType()),               // MRI slice ordering
      AttrType("dim", CollectionType(IntType())),       // data array dimensions -> up to 8 integers

      AttrType("intent", CollectionType(FloatType())),  // -> 3 intent parameters (
      AttrType("intent_code", IntType()),               // NIFTI_INTENT_* code
      AttrType("intent_name", StringType()),            // * text value corresponding to intent_code

      AttrType("datatype", IntType()),                  // defines data type!
      AttrType("datatype_name", StringType()),          // defines data type!

      AttrType("bitpix", IntType()),                    // number of bits per voxel (cf. datatype)

      AttrType("slice_start", IntType()),               // first slice index
      AttrType("pixdim", CollectionType(FloatType())),  // grid spacing
      AttrType("vox_offset", FloatType()),              // offset into .nii file
      AttrType("scl_slope", FloatType()),               // data scaling: slope
      AttrType("scl_inter", FloatType()),               // data scaling: offset
      AttrType("slice_end", IntType()),                 // last slice index

      AttrType("slice_code", IntType()),                // slice timing order
      AttrType("xyzt_units", IntType()),                // units of pixdim[1..4]

      AttrType("cal_max", FloatType()),                 // max display intensity
      AttrType("cal_min", FloatType()),                 // min display intensity
      AttrType("slice_duration", FloatType()),          // time for 1 slice
      AttrType("toffset", FloatType()),                 // time axis shift

      AttrType("glmax", IntType()),                     // UNUSED
      AttrType("glmin", IntType()),                     // UNUSED

      AttrType("descrip", StringType()),                // any text you like
      AttrType("aux_file", StringType()),               // auxiliary filename

      AttrType("qform_code", IntType()),                // NIFTI_XFORM_* code
      AttrType("sform_code", IntType()),                // NIFTI_XFORM_* code

      AttrType("quatern", CollectionType(FloatType())), // quaternion b,c,d parameters
      AttrType("qoffset", CollectionType(FloatType())), // quaternion x,y,z shift
      AttrType("srow_x", CollectionType(FloatType())),  // 1st row affine transform
      AttrType("srow_y", CollectionType(FloatType())),  // 2nd row affine transform
      AttrType("srow_z", CollectionType(FloatType())),  // 3rd row affine transform

      AttrType("magic", StringType()),                  // MUST be "ni1\0" or "n+1\0"

      AttrType("extension", CollectionType(IntType()))
    ))
  }
}