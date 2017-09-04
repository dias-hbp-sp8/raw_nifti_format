package raw.executor.spark.inputformats.nifti

/**
  * Created by dperez on 30.08.17.
  *
  * NiftiRecordParserBuilder
  *
  * Creates the code necessary for parsing a NIFTI file
  * - uses NiftiHeader to parse the header
  * - creates a dedicated loop to read voxel values
  *
  */

import java.util.concurrent.atomic.AtomicInteger

import raw.compiler.L0.source.ListType
import raw.compiler.base.source.{AttrType, RecordType, Type}
import raw.executor.spark.{CodeGenClassCode, CodeGenContext}
import raw.inputformats.InputFormatDescriptor
import raw.inputformats.nifti.{NiftiHeader, NiftiInputFormatDescriptor}

import scala.collection.mutable.ArrayBuffer


object NiftiRecordParserBuilder {

  private val ai = new AtomicInteger() // Global counter (across queries)

  def apply(format: InputFormatDescriptor, innerType: Type)(implicit codeGenContext: CodeGenContext): CodeGenClassCode = {

    val NiftiInputFormatDescriptor(littleEndian, datatype, bitpix, nDim) = format
    val scalaType = codeGenContext.buildType(innerType)


    // Generate code to read the header and/or data based on the
    // expected output type ("innerType")

    def build(): CodeGenClassCode = {

      // A NIFTI output type should always be a RecordType containing either or both:
      // - a "header" part
      // - a "data" part

      val innerTypeAttrVector:Vector[AttrType] = innerType match {
        case RecordType(atts) => atts
        case _ => throw new RuntimeException(s"Unexpected innerType: $innerType")
      }

      val attsNameAndCode:Vector[(String,String)] = for (att <- innerTypeAttrVector) yield att.idn match {
        case "header" => {
          val hdrType = att.tipe
          val hdrClassName = codeGenContext.buildType(hdrType)
          println("hdrClassName: "+ hdrClassName)
          ("header", s"val header = ${codeGenHeaderBuilder(att.tipe)}")
        }
        case "data" => {
          ("data",
            s"""|val asReadData = ${addReadVoxelLoop('i',nDim,att.tipe)}
                |val data = ${codeGenDataBuilder}
             """.stripMargin)
        }
        case _ => throw new RuntimeException(s"Unexpected attribute of innerType: $att")
      }

      val attsCode = (attsNameAndCode map {elem => elem._2}).mkString("\n\t")

      val newRecordAtts = (attsNameAndCode map {elem => elem._1}).mkString(",")
      val newRecordCode = s"new $scalaType($newRecordAtts)"


      val className = s"NiftiRecordParser${ai.getAndIncrement()}"

      val code =
        s"""|class $className extends NiftiRecordParser[$scalaType] {
            |
            | override def parse(hdr: NiftiHeader, di: DataInput): $scalaType = {
            |   $attsCode
            |   $newRecordCode
            | }
            |}
        """.stripMargin
      codeGenContext.addClass(className, code)
      CodeGenClassCode(className, code)

    }

    // Generate code to read the header and return the expected type

    def codeGenHeaderBuilder(record: Type): String = {

      val hdrClassName = codeGenContext.buildType(record)

      val RecordType(atts: Vector[AttrType]) = record
      val code =
        if (atts.isEmpty) throw new RuntimeException(s"Unexpected innerType: $innerType")
        else {
          val fieldDeclarations = new ArrayBuffer[String]()
          atts.foreach {
            case att => fieldDeclarations += s"hdr.${att.idn}"
          }

          s"new $hdrClassName(${fieldDeclarations.mkString(",")})"
        }
      code
    }

    // Generate code to read the voxel data in the right order
    // and return the expected type

    def codeGenDataBuilder: String = {
      addTransposeLoop('i',1) + List.fill(nDim)("}").mkString("")
    }

    def addTransposeLoop(it:Char, dim:Int): String = {
      if (dim > nDim) "asReadData"
      else
        "for(" + it + " <- 0 until hdr.dim(" + (dim) + ")) yield {" + addTransposeLoop((it + 1).toChar, dim + 1) + "(" + it + ")"
    }


    def addReadVoxelLoop(it:Char, dim:Int, dataType:Type): String = {
      if (dim == 0) codeGenReadVoxel()
      else
        dataType match {
          case ListType(subType:Type) =>
            "for(" + it + " <- 0 until hdr.dim(" + dim + ")) yield {" + addReadVoxelLoop((it + 1).toChar, dim - 1, subType) + "}"
          case _ => throw new RuntimeException(s"Unexpected data type structure: $dataType")
        }
    }

    def codeGenReadVoxel() : String = {
      // apply scaling: if (hdr.scl_slope != 0) v = v0 * hdr.scl_slope + hdr.scl_inter
      (datatype,bitpix) match {
        case (NiftiHeader.NIFTI_TYPE_INT8,8) =>
          "di.readByte.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_UINT8,8) =>                         // checked
          "di.readUnsignedByte.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_INT16,16) =>                        // checked
          "di.readShort.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_UINT16,16) =>                       // checked
          "di.readUnsignedShort.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_INT32,32) =>                      //
          "di.readInt.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_UINT32,32) =>                       //
          """{val read = di.readInt.toDouble
            |   val touint = if(read <0)(read + (1.toLong << 32).toDouble).toFloat else read.toFloat
            |   touint * hdr.scl_slope + hdr.scl_inter} """
        case (NiftiHeader.NIFTI_TYPE_INT64,64) =>                        //
          "di.readLong.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_FLOAT32,32) =>                      // checked
          "di.readFloat.toFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.DT_BINARY,1) =>                                // ignore scaling
          "{val read=di.readBoolean; if (read) 1:Float else 0:Float}"
        //case (NiftiHeader.DT_BINARY,1) => "di.readBoolean.toFloat"
        case (NiftiHeader.NIFTI_TYPE_COMPLEX64, 64) => // create one more for loop (a = 0,1)
          "for(a <- 0 until 2) di.readFloat * hdr.scl_slope + hdr.scl_inter"
        case (NiftiHeader.NIFTI_TYPE_RGB24, 24) => // one more for loop (a = 0,1,2), scaling must be ignored
          "for(a <- 0 until 3) di.readUnsignedByte"
        case _ => // For the remaining datatypes, NiftiInferrer should have already thrown an exception
          throw new RuntimeException("Sorry, cannot yet read nifti-1 datatype " + NiftiHeader.decodeDatatype(datatype))
      }
    }

    build()
  }

}
