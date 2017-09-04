package raw.inputformats.nifti

/**
  * Created by dperez on 28.08.17.
  */

import java.io._
import java.nio.ByteOrder
import java.util.Date
import java.util.zip.GZIPInputStream

import com.google.common.io.{LittleEndianDataInputStream, LittleEndianDataOutputStream}

import scala.collection.mutable.HashMap


/**
  * Created by dperez on 27.03.17.
  * Code derived from niftijio, itself derived from http://niftilib.sourceforge.net/
  */

class NiftiException(message: String) extends Exception

class NiftiHeader {

  var filename:String = _
  var little_endian = false
  var freq_dim = 0
  var phase_dim = 0
  var slice_dim = 0
  var xyz_unit_code:Int = 0
  var t_unit_code:Int = 0
  var qfac = 0
  var extensions_list: Seq[Seq[Int]] = Nil
  var extension_blobs: Seq[Seq[Int]] = Nil

  var sizeof_hdr = 0
  var data_type_string: String = _
  var db_name: String = _
  var extents = 0
  var session_error = 0
  var regular: String = _
  var dim_info: String = _
  var dim = Array[Int](8)
  var intent = Array[Float](3)
  var intent_code = 0
  var datatype = 0
  var datatype_name = ""
  var bitpix = 0
  var slice_start = 0
  var pixdim = new Array[Float](8)
  var vox_offset: Float = _
  var scl_slope: Float = _
  var scl_inter: Float = _
  var slice_end = 0
  var slice_code:Byte = 0
  var xyzt_units:Byte = 0
  var cal_max: Float = _
  var cal_min: Float = _
  var slice_duration: Float = _
  var toffset: Float = _
  var glmax = 0
  var glmin = 0
  var descrip: String = _
  var aux_file: String = _
  var qform_code = 0
  var sform_code = 0
  var quatern = new Array[Float](3)
  var qoffset = new Array[Float](3)
  var srow_x = new Array[Float](4)
  var srow_y = new Array[Float](4)
  var srow_z = new Array[Float](4)
  var intent_name: String = _
  var magic: String = _
  var extension = new Array[Int](4)

  setDefaults()

  def this(nx: Int, ny: Int, nz: Int, dim: Int) {
    this()
    setDefaults()
    this.filename = ""
    this.pixdim(0) = 1.0f
    this.pixdim(1) = 1.0f
    this.pixdim(2) = 1.0f
    this.srow_x(0) = 1.0f
    this.srow_y(1) = 1.0f
    this.srow_z(2) = 1.0f
    this.descrip = "Created: " + new Date().toString
    this.setDatatype(NiftiHeader.NIFTI_TYPE_FLOAT32)
    this.dim(0) = (if (dim > 1) 4 else 3)
    this.dim(1) = nx
    this.dim(2) = ny
    this.dim(3) = nz
    this.dim(4) = (if (dim > 1) dim else 0)
  }

  def setDatatype(code: Int): Unit = {
    datatype = code
    bitpix = (NiftiHeader.bytesPerVoxel(code) * 8)
  }

  private def setDefaults() = {
    little_endian = ByteOrder.nativeOrder eq ByteOrder.LITTLE_ENDIAN
    sizeof_hdr = NiftiHeader.ANZ_HDR_SIZE
    data_type_string = ""
    var i = 0
    data_type_string = ((0 until 10) map (_ => "\u0000")).mkString
    db_name = ((0 until 18) map (_ => "\u0000")).mkString
    extents = 0
    session_error = 0
    regular = "\u0000"
    dim_info = "\u0000"
    freq_dim = 0
    phase_dim = 0
    slice_dim = 0
    dim = new Array[Int](8) map (_ => 0)
    //dim[1] = 0;
    //dim[2] = 0;
    //dim[3] = 0;
    //dim[4] = 0;
    intent = new Array[Float](3) map (_ => 0f)
    intent_code = NiftiHeader.NIFTI_INTENT_NONE
    datatype = NiftiHeader.DT_NONE
    bitpix = 0
    slice_start = 0
    pixdim = Array[Float](1,0,0,0,0,0,0,0)
    qfac = 1
    vox_offset = 352f
    scl_slope = 0f
    scl_inter = 0f
    slice_end = 0
    slice_code = 0:Byte
    xyzt_units = 0:Byte
    xyz_unit_code = NiftiHeader.NIFTI_UNITS_UNKNOWN
    t_unit_code = NiftiHeader.NIFTI_UNITS_UNKNOWN
    cal_max = 0f
    cal_min = 0f
    slice_duration = 0f
    toffset = 0f
    glmax = 0
    glmin = 0
    descrip = ((0 until 80) map (_ => "\u0000")).mkString
    aux_file = ((0 until 24) map (_ => "\u0000")).mkString
    qform_code = NiftiHeader.NIFTI_XFORM_UNKNOWN
    sform_code = NiftiHeader.NIFTI_XFORM_UNKNOWN
    quatern = new Array[Float](3) map (_ => 0f)
    qoffset = new Array[Float](3) map (_ => 0f)
    srow_x = new Array[Float](4) map (_ => 0f)
    srow_y = new Array[Float](4) map (_ => 0f)
    srow_z = new Array[Float](4) map (_ => 0f)
    intent_name = ((0 until 16) map (_ => "\u0000")).mkString
    magic = NiftiHeader.NII_MAGIC_STRING
    extension = new Array[Int](4) map (_ => 0)

    extensions_list = List()
    extension_blobs = List()
  }

  def info: HashMap[String, String] = {
    var res = HashMap[String, String](
      "size" -> String.valueOf(sizeof_hdr),
      "data_offset" -> String.valueOf(vox_offset),
      "magic_string" -> String.valueOf(magic),
      "datatype_code" -> String.valueOf(datatype),
      "datatype_name" -> NiftiHeader.decodeDatatype(datatype),
      "bit_per_vox" -> String.valueOf(bitpix),
      "scaling_offset" -> String.valueOf(scl_inter),
      "scaling_slope" -> String.valueOf(scl_slope),
      "xyz_units_code" -> String.valueOf(xyz_unit_code),
      "xyz_units_name" -> NiftiHeader.decodeUnits(xyz_unit_code),
      "t_units_code" -> String.valueOf(t_unit_code),
      "t_units_name" -> NiftiHeader.decodeUnits(t_unit_code),
      "t_offset" -> String.valueOf(toffset),
      "intent_code" -> String.valueOf(intent_code),
      "intent_name" -> NiftiHeader.decodeIntent(intent_code),
      "cal_max" -> String.valueOf(cal_max),
      "cal_min" -> String.valueOf(cal_min),
      "slice_code" -> String.valueOf(slice_code),
      "slice_name" -> NiftiHeader.decodeSliceOrder(slice_code),
      "slice_freq" -> String.valueOf(freq_dim),
      "slice_phase" -> String.valueOf(phase_dim),
      "slice_index" -> String.valueOf(slice_dim),
      "slice_start" -> String.valueOf(slice_start),
      "slice_end" -> String.valueOf(slice_end),
      "slice_dur" -> String.valueOf(slice_duration),
      "qfac" -> String.valueOf(qfac),
      "qform_code" -> String.valueOf(qform_code),
      "qform_name" -> NiftiHeader.decodeXform(qform_code),
      "quat_b" -> String.valueOf(quatern(0)),
      "quat_c" -> String.valueOf(quatern(1)),
      "quat_d" -> String.valueOf(quatern(2)),
      "quat_x" -> String.valueOf(qoffset(0)),
      "quat_y" -> String.valueOf(qoffset(1)),
      "quat_z" -> String.valueOf(qoffset(2)),
      "sform_code" -> String.valueOf(sform_code),
      "sform_name" -> NiftiHeader.decodeXform(sform_code))
    //"computed orientation" -> this.orientation)
    for (i <- (0 to dim(0))) res += ("dim"+i -> String.valueOf(dim(i)))
    for (i <- (0 to dim(0))) res += ("space"+i -> String.valueOf(pixdim(i)))
    for (i <- (0 until 3)) res += ("intent"+i -> String.valueOf(intent(i)))
    for (i <- (0 until 4)) res += ("sform0"+i -> String.valueOf(srow_x(i)))
    for (i <- (0 until 4)) res += ("sform1"+i -> String.valueOf(srow_y(i)))
    for (i <- (0 until 4)) res += ("sform2"+i -> String.valueOf(srow_z(i)))
    res
  }

  def dump(): Unit = {
    dump(new PrintWriter(System.out))
  }

  def dump(writer: PrintWriter): Unit = {
    val attrs = info
    for ((key,value) <- attrs)
      writer.println(key + ": " + value)
  }

  private def setStringSize(s: String, n: Int): Array[Byte] = {
    val slen = s.length
    if (slen >= n) s.substring(0, n).getBytes
    else ((0 until n) map (i => if (i < slen) s(i).toByte else 0:Byte)).toArray
  }

  @throws[IOException]
  def encodeHeader: Array[Byte] = {
    val os = new ByteArrayOutputStream
    val dout = if (little_endian) new LittleEndianDataOutputStream(os) else new DataOutputStream(os)

    dout.writeInt(sizeof_hdr)
    dout.write(setStringSize(data_type_string,10), 0, 10)
    dout.write(setStringSize(db_name,18), 0, 18)
    dout.writeInt(extents)
    dout.writeShort(session_error)
    dout.writeByte(regular.charAt(0).toInt)
    var spf_dims = 0
    spf_dims = (spf_dims & (slice_dim & 3)) << 2
    spf_dims = (spf_dims & (phase_dim & 3)) << 2
    spf_dims = spf_dims & ((freq_dim) & 3)
    val b = spf_dims.toByte
    dout.writeByte(b.toInt)
    for (i <- 0 until 8) dout.writeShort(dim(i))
    for (i <- 0 until 3) dout.writeFloat(intent(i))
    dout.writeShort(intent_code)
    dout.writeShort(datatype)
    dout.writeShort(bitpix)
    dout.writeShort(slice_start)
    for (i <- 0 until 8) dout.writeFloat(pixdim(i))

    dout.writeFloat(vox_offset)
    dout.writeFloat(scl_slope)
    dout.writeFloat(scl_inter)
    dout.writeShort(slice_end)
    dout.writeByte(slice_code.toInt)
    val units = ((xyz_unit_code & 7:Int) | (t_unit_code & 70:Int)).toByte
    dout.writeByte(units)
    dout.writeFloat(cal_max)
    dout.writeFloat(cal_min)
    dout.writeFloat(slice_duration)
    dout.writeFloat(toffset)
    dout.writeInt(glmax)
    dout.writeInt(glmin)
    dout.write(setStringSize(descrip, 80), 0, 80)
    dout.write(setStringSize(aux_file, 24), 0, 24)
    dout.writeShort(qform_code)
    dout.writeShort(sform_code)
    for (i <- 0 until 3) dout.writeFloat(quatern(i))
    for (i <- 0 until 3) dout.writeFloat(qoffset(i))
    for (i <- 0 until 4) dout.writeFloat(srow_x(i))
    for (i <- 0 until 4) dout.writeFloat(srow_y(i))
    for (i <- 0 until 4) dout.writeFloat(srow_z(i))
    dout.write(setStringSize(intent_name, 16), 0, 16)
    dout.write(setStringSize(magic, 4), 0, 4)
    if (this.extension(0) != 0) {
      for (i <- 0 until 4) dout.writeByte(extension(i))
      for (i <- 0 until extensions_list.size){
        val size_code = extensions_list(i)
        dout.writeInt(size_code(0))
        dout.writeInt(size_code(1))
        val a: Array[Byte] = extension_blobs(i).toArray map {case a:Int => a.toByte}
        dout.write(a)
      }
    }
    if (this.little_endian) dout.asInstanceOf[LittleEndianDataOutputStream].close()
    else dout.asInstanceOf[DataOutputStream].close()
    os.toByteArray
  }
  /*
    def orientation: String = {
      val mat44 = this.mat44
      var `val` = .0
      var detQ = .0
      var detP = .0
      val P = new Array[Array[Double]](3, 3)
      val Q = new Array[Array[Double]](3, 3)
      var M = new Array[Array[Double]](3, 3)
      var i = 0
      var j = 0
      var k = 0
      var p = 0
      var q = 0
      var r = 0
      var ibest = 0
      var jbest = 0
      var kbest = 0
      var pbest = 0
      var qbest = 0
      var rbest = 0
      var vbest = .0
      var xi = mat44(0)(0)
      var xj = mat44(0)(1)
      var xk = mat44(0)(2)
      var yi = mat44(1)(0)
      var yj = mat44(1)(1)
      var yk = mat44(1)(2)
      var zi = mat44(2)(0)
      var zj = mat44(2)(1)
      var zk = mat44(2)(2)
      // normalize column vectors to get unit vectors along each ijk-axis
      // normalize i axis
      `val` = Math.sqrt(xi * xi + yi * yi + zi * zi)
      if (`val` == 0.0) throw new RuntimeException("invalid nifti transform")
      xi /= `val`
      yi /= `val`
      zi /= `val`
      // normalize j axis
      `val` = Math.sqrt(xj * xj + yj * yj + zj * zj)
      if (`val` == 0.0) throw new RuntimeException("invalid nifti transform")
      xj /= `val`
      yj /= `val`
      zj /= `val`
      // orthogonalize j axis to i axis, if needed
      `val` = xi * xj + yi * yj + zi * zj // dot product between i and j

      if (Math.abs(`val`) > 1.e-4) {
        xj -= `val` * xi
        yj -= `val` * yi
        zj -= `val` * zi
        `val` = Math.sqrt(xj * xj + yj * yj + zj * zj) // renormalize

        if (`val` == 0.0) { // j was parallel to i?
          throw new RuntimeException("invalid nifti transform")
        }
        xj /= `val`
        yj /= `val`
        zj /= `val`
      }
      // normalize k axis; if it is zero, make it the cross product i x j
      `val` = Math.sqrt(xk * xk + yk * yk + zk * zk)
      if (`val` == 0.0) {
        xk = yi * zj - zi * yj
        yk = zi * xj - zj * xi
        zk = xi * yj - yi * xj
      }
      else {
        xk /= `val`
        yk /= `val`
        zk /= `val`
      }
      // orthogonalize k to i
      `val` = xi * xk + yi * yk + zi * zk /* dot product between i and k */
      if (Math.abs(`val`) > 1.e-4) {
        xk -= `val` * xi
        yk -= `val` * yi
        zk -= `val` * zi
        `val` = Math.sqrt(xk * xk + yk * yk + zk * zk)
        if (`val` == 0.0) throw new RuntimeException("invalid nifti transform") /* bad */
        xk /= `val`
        yk /= `val`
        zk /= `val`
      }
      // orthogonalize k to j
      `val` = xj * xk + yj * yk + zj * zk // dot product between j and k

      if (Math.abs(`val`) > 1.e-4) {
        xk -= `val` * xj
        yk -= `val` * yj
        zk -= `val` * zj
        `val` = Math.sqrt(xk * xk + yk * yk + zk * zk)
        if (`val` == 0.0) throw new RuntimeException("invalid nifti transform")
        xk /= `val`
        yk /= `val`
        zk /= `val`
      }
      Q(0)(0) = xi
      Q(0)(1) = xj
      Q(0)(2) = xk
      Q(1)(0) = yi
      Q(1)(1) = yj
      Q(1)(2) = yk
      Q(2)(0) = zi
      Q(2)(1) = zj
      Q(2)(2) = zk
      // now Q is the rotation matrix from the (i,j,k) to (x,y,z) axes
      detQ = det33(Q)
      if (detQ == 0.0) throw new RuntimeException("invalid nifti transform")
      /*
               * Build and test all possible +1/-1 coordinate permutation matrices P;
               * then find the P such that the rotation matrix M=PQ is closest to the
               * identity, in the sense of M having the smallest total rotation angle.
               *//*
               * Despite the formidable looking 6 nested loops, there are only
               * 3*3*3*2*2*2 = 216 passes, which will run very quickly.
               */ vbest = -666.0
      ibest = pbest = qbest = rbest = 1
      jbest = 2
      kbest = 3
      i = 1
      while ( {
        i <= 3
      }) {
        /* i = column number to use for row #1 */ j = 1
        while ( {
          j <= 3
        }) {
          /* j = column number to use for row #2 */ if (i == j) {
            continue //todo: continue is not supported}
            k = 1
            while ( {
              k <= 3
            }) {
              /* k = column number to use for row #3 */ if (i == k || j == k) {
                continue //todo: continue is not supported}
                P(0)(0) = P(0)(1) = P(0)(2) = P(1)(0) = P(1)(1) = P(1)(2) = P(2)(0) = P(2)(1) = P(2)(2) = 0.0
                p = -1
                while ( {
                  p <= 1
                }) {
                  /* p,q,r are -1 or +1 */ q = -1
                  while ( {
                    q <= 1
                  }) {
                    /* and go into rows #1,2,3 */ r = -1
                    while ( {
                      r <= 1
                    }) {
                      P(0)(i - 1) = p
                      P(1)(j - 1) = q
                      P(2)(k - 1) = r
                      detP = det33(P) // permutation

                      // sign
                      if (detP * detQ <= 0.0) {
                        continue //todo: continue is not supported/* doesn't match sign of Q */}
                        M = mult(P, Q)
                        /*
                                                         * angle of M rotation =
                                                         * 2.0*acos(0.5*sqrt(1.0+trace(M)))
                                                         *//*
                                                         * we want largest trace(M) == smallest angle ==
                                                         * M nearest to I
                                                         */ `val` = M(0)(0) + M(1)(1) + M(2)(2) /* trace */
                        if (`val` > vbest) {
                          vbest = `val`
                          ibest = i
                          jbest = j
                          kbest = k
                          pbest = p
                          qbest = q
                          rbest = r
                        }

                        r += 2
                      }

                      q += 2
                    }

                    p += 2
                  }

                  {
                    k += 1; k - 1
                  }
                }

                {
                  j += 1; j - 1
                }
              }

              {
                i += 1; i - 1
              }
            }
            /*
                     * At this point ibest is 1 or 2 or 3; pbest is -1 or +1; etc.
                     *
                     * The matrix P that corresponds is the best permutation approximation
                     * to Q-inverse; that is, P (approximately) takes (x,y,z) coordinates to
                     * the (i,j,k) axes.
                     *
                     * For example, the first row of P (which contains pbest in column
                     * ibest) determines the way the i axis points relative to the
                     * anatomical (x,y,z) axes. If ibest is 2, then the i axis is along the
                     * y axis, which is direction P2A (if pbest > 0) or A2P (if pbest < 0).
                     *
                     * So, using ibest and pbest, we can assign the output code for the i
                     * axis. Mutatis mutandis for the j and k axes, of course.
                     */ ibest * pbest match {
              case 1 =>
                i = NIFTI_L2R
                break //todo: break is not supported
              case -(1) =>
                i = NIFTI_R2L
                break //todo: break is not supported
              case 2 =>
                i = NIFTI_P2A
                break //todo: break is not supported
              case -(2) =>
                i = NIFTI_A2P
                break //todo: break is not supported
              case 3 =>
                i = NIFTI_I2S
                break //todo: break is not supported
              case -(3) =>
                i = NIFTI_S2I
                break //todo: break is not supported
            }
            jbest * qbest match {
              case 1 =>
                j = NIFTI_L2R
                break //todo: break is not supported
              case -(1) =>
                j = NIFTI_R2L
                break //todo: break is not supported
              case 2 =>
                j = NIFTI_P2A
                break //todo: break is not supported
              case -(2) =>
                j = NIFTI_A2P
                break //todo: break is not supported
              case 3 =>
                j = NIFTI_I2S
                break //todo: break is not supported
              case -(3) =>
                j = NIFTI_S2I
                break //todo: break is not supported
            }
            kbest * rbest match {
              case 1 =>
                k = NIFTI_L2R
                break //todo: break is not supported
              case -(1) =>
                k = NIFTI_R2L
                break //todo: break is not supported
              case 2 =>
                k = NIFTI_P2A
                break //todo: break is not supported
              case -(2) =>
                k = NIFTI_A2P
                break //todo: break is not supported
              case 3 =>
                k = NIFTI_I2S
                break //todo: break is not supported
              case -(3) =>
                k = NIFTI_S2I
                break //todo: break is not supported
            }
            return NIFTI_ORIENT(i - 1) + NIFTI_ORIENT(j - 1) + NIFTI_ORIENT(k - 1)
          }

          def det33(R: Array[Array[Double]]) = { // determinant of a 3x3 matrix
            val r11 = R(0)(0)
            val r12 = R(0)(1)
            val r13 = R(0)(2)
            val r21 = R(1)(0)
            val r22 = R(1)(1)
            val r23 = R(1)(2)
            val r31 = R(2)(0)
            val r32 = R(2)(1)
            val r33 = R(2)(2)
            r11 * r22 * r33 - r11 * r32 * r23 - r21 * r12 * r33 + r21 * r32 * r13 + r31 * r12 * r23 - r31 * r22 * r13
          }

          def mat44 = if (this.sform_code > 0) sform_to_mat44
          else if (this.qform_code > 0) qform_to_mat44
            else {
              val out = new Array[Array[Double]](4, 4)
              out(0)(0) = this.pixdim(1)
              out(1)(1) = this.pixdim(2)
              out(2)(2) = this.pixdim(3)
              out(3)(3) = 1.0
              out
            }

          def sform_to_mat44 = {
            val out = new Array[Array[Double]](4, 4)
            var i = 0
            while ( {
              i < 4
            }) {
              out(0)(i) = this.srow_x(i)
              out(1)(i) = this.srow_y(i)
              out(2)(i) = this.srow_z(i)
              out(3)(i) = 0

              {
                i += 1; i - 1
              }
            }
            out(3)(3) = 1.0
            out
          }

          def qform_to_mat44 = {
            val qb = this.quatern(0)
            val qc = this.quatern(1)
            val qd = this.quatern(2)
            val qx = this.qoffset(0)
            val qy = this.qoffset(1)
            val qz = this.qoffset(2)
            val dx = this.pixdim(1)
            val dy = this.pixdim(2)
            val dz = this.pixdim(3)
            val qfac = this.qfac
            val R = new Array[Array[Double]](4, 4)
            /* last row is always [ 0 0 0 1 ] */ R(3)(0) = R(3)(1) = R(3)(2) = 0.0
            R(3)(3) = 1.0
            /* compute a parameter from b,c,d */ var d = qd
            var c = qc
            var b = qb
            var a = 1.0 - (b * b + c * c + d * d)
            if (a < 1.e-7) {
              /* special case */ a = 1.0 / Math.sqrt(b * b + c * c + d * d)
              b *= a
              c *= a
              d *= a /* normalize (b,c,d) vector */
              a = 0.0 /* a = 0 ==> 180 degree rotation */
            }
            else a = Math.sqrt(a) /* angle = 2*arccos(a) */
            /* load rotation matrix, including scaling factors for voxel sizes */ val xd = if (dx > 0.0) dx
            else 1.0
            /* make sure are positive */
            val yd = if (dy > 0.0) dy
            else 1.0
            var zd = if (dz > 0.0) dz
            else 1.0
            if (qfac < 0.0) zd = -zd /* left handedness? */
            R(0)(0) = (a * a + b * b - c * c - d * d) * xd
            R(0)(1) = 2.0 * (b * c - a * d) * yd
            R(0)(2) = 2.0 * (b * d + a * c) * zd
            R(1)(0) = 2.0 * (b * c + a * d) * xd
            R(1)(1) = (a * a + c * c - b * b - d * d) * yd
            R(1)(2) = 2.0 * (c * d - a * b) * zd
            R(2)(0) = 2.0 * (b * d - a * c) * xd
            R(2)(1) = 2.0 * (c * d + a * b) * yd
            R(2)(2) = (a * a + d * d - c * c - b * b) * zd
            /* load offsets */ R(0)(3) = qx
            R(1)(3) = qy
            R(2)(3) = qz
            R
          }

          def update_sform() = {
            val mat44 = qform_to_mat44
            var i = 0
            while ( {
              i < 4
            }) {
              this.srow_x(i) = mat44(0)(i).toFloat
              this.srow_y(i) = mat44(1)(i).toFloat
              this.srow_z(i) = mat44(2)(i).toFloat

              {
                i += 1; i - 1
              }
            }
            this.sform_code = 1
          }

          private def mult(A: Array[Array[Double]], B: Array[Array[Double]]): Array[Array[Double]]

          =
          { // multiply 3x3 matrices
            val C = new Array[Array[Double]](3, 3)
            var i = 0
            var j = 0
            i = 0
            while ( {
              i < 3
            }) {
              j = 0
              while ( {
                j < 3
              }) C(i)(j) = A(i)(0) * B(0)(j) + A(i)(1) * B(1)(j) + A(i)(2) * B(2)(j) {
                j += 1; j - 1
              }
              {
                i += 1; i - 1
              }
            }
            return C
          }
  */
}


object NiftiHeader {

  val ANZ_HDR_SIZE = 348
  val EXT_KEY_SIZE = 8
  val NII_MAGIC_STRING = "n+1"

  val NIFTI_INTENT_NONE = 0
  val NIFTI_INTENT_CORREL = 2
  val NIFTI_INTENT_TTEST = 3
  val NIFTI_INTENT_FTEST = 4
  val NIFTI_INTENT_ZSCORE = 5
  val NIFTI_INTENT_CHISQ = 6
  val NIFTI_INTENT_BETA = 7
  val NIFTI_INTENT_BINOM = 8
  val NIFTI_INTENT_GAMMA = 9
  val NIFTI_INTENT_POISSON = 10
  val NIFTI_INTENT_NORMAL = 11
  val NIFTI_INTENT_FTEST_NONC = 12
  val NIFTI_INTENT_CHISQ_NONC = 13
  val NIFTI_INTENT_LOGISTIC = 14
  val NIFTI_INTENT_LAPLACE = 15
  val NIFTI_INTENT_UNIFORM = 16
  val NIFTI_INTENT_TTEST_NONC = 17
  val NIFTI_INTENT_WEIBULL = 18
  val NIFTI_INTENT_CHI = 19
  val NIFTI_INTENT_INVGAUSS = 20
  val NIFTI_INTENT_EXTVAL = 21
  val NIFTI_INTENT_PVAL = 22
  val NIFTI_INTENT_ESTIMATE = 1001
  val NIFTI_INTENT_LABEL = 1002
  val NIFTI_INTENT_NEURONAME = 1003
  val NIFTI_INTENT_GENMATRIX = 1004
  val NIFTI_INTENT_SYMMATRIX = 1005
  val NIFTI_INTENT_DISPVECT = 1006
  val NIFTI_INTENT_VECTOR = 1007
  val NIFTI_INTENT_POINTSET = 1008
  val NIFTI_INTENT_TRIANGLE = 1009
  val NIFTI_INTENT_QUATERNION = 1010

  val DT_NONE = 0
  val DT_BINARY = 1
  val NIFTI_TYPE_UINT8 = 2
  val NIFTI_TYPE_INT16 = 4
  val NIFTI_TYPE_INT32 = 8
  val NIFTI_TYPE_FLOAT32 = 16
  val NIFTI_TYPE_COMPLEX64 = 32
  val NIFTI_TYPE_FLOAT64 = 64
  val NIFTI_TYPE_RGB24 = 128
  val DT_ALL = 255
  val NIFTI_TYPE_INT8 = 256
  val NIFTI_TYPE_UINT16 = 512
  val NIFTI_TYPE_UINT32 = 768
  val NIFTI_TYPE_INT64 = 1024
  val NIFTI_TYPE_UINT64 = 1280
  val NIFTI_TYPE_FLOAT128 = 1536
  val NIFTI_TYPE_COMPLEX128 = 1792
  val NIFTI_TYPE_COMPLEX256 = 2048
  val NIFTI_TYPE_RGBA32 = 2304

  val NIFTI_UNITS_UNKNOWN = 0
  val NIFTI_UNITS_METER = 1
  val NIFTI_UNITS_MM = 2
  val NIFTI_UNITS_MICRON = 3
  val NIFTI_UNITS_SEC = 8
  val NIFTI_UNITS_MSEC = 16
  val NIFTI_UNITS_USEC = 24
  val NIFTI_UNITS_HZ = 32
  val NIFTI_UNITS_PPM = 40
  val NIFTI_UNITS_RADS = 48

  val NIFTI_SLICE_SEQ_INC = 1
  val NIFTI_SLICE_SEQ_DEC = 2
  val NIFTI_SLICE_ALT_INC = 3
  val NIFTI_SLICE_ALT_DEC = 4

  val NIFTI_XFORM_UNKNOWN = 0
  val NIFTI_XFORM_SCANNER_ANAT = 1
  val NIFTI_XFORM_ALIGNED_ANAT = 2
  val NIFTI_XFORM_TALAIRACH = 3
  val NIFTI_XFORM_MNI_152 = 4

  val NIFTI_L2R = 1
  val NIFTI_R2L = 2
  val NIFTI_P2A = 3
  val NIFTI_A2P = 4
  val NIFTI_I2S = 5
  val NIFTI_S2I = 6

  val NIFTI_ORIENT = Array("L", "R", "P", "A", "I", "S")

  def decodeIntent(icode: Int): String = icode match {
    case NIFTI_INTENT_NONE => "NIFTI_INTENT_NONE"
    case NIFTI_INTENT_CORREL => "NIFTI_INTENT_CORREL"
    case NIFTI_INTENT_TTEST => "NIFTI_INTENT_TTEST"
    case NIFTI_INTENT_FTEST => "NIFTI_INTENT_FTEST"
    case NIFTI_INTENT_ZSCORE => "NIFTI_INTENT_ZSCORE"
    case NIFTI_INTENT_CHISQ => "NIFTI_INTENT_CHISQ"
    case NIFTI_INTENT_BETA => "NIFTI_INTENT_BETA"
    case NIFTI_INTENT_BINOM => "NIFTI_INTENT_BINOM"
    case NIFTI_INTENT_GAMMA => "NIFTI_INTENT_GAMMA"
    case NIFTI_INTENT_POISSON => "NIFTI_INTENT_POISSON"
    case NIFTI_INTENT_NORMAL => "NIFTI_INTENT_NORMAL"
    case NIFTI_INTENT_FTEST_NONC => "NIFTI_INTENT_FTEST_NONC"
    case NIFTI_INTENT_CHISQ_NONC => "NIFTI_INTENT_CHISQ_NONC"
    case NIFTI_INTENT_LOGISTIC => "NIFTI_INTENT_LOGISTIC"
    case NIFTI_INTENT_LAPLACE => "NIFTI_INTENT_LAPLACE"
    case NIFTI_INTENT_UNIFORM => "NIFTI_INTENT_UNIFORM"
    case NIFTI_INTENT_TTEST_NONC => "NIFTI_INTENT_TTEST_NONC"
    case NIFTI_INTENT_WEIBULL => "NIFTI_INTENT_WEIBULL"
    case NIFTI_INTENT_CHI => "NIFTI_INTENT_CHI"
    case NIFTI_INTENT_INVGAUSS => "NIFTI_INTENT_INVGAUSS"
    case NIFTI_INTENT_EXTVAL => "NIFTI_INTENT_EXTVAL"
    case NIFTI_INTENT_PVAL => "NIFTI_INTENT_PVAL"
    case NIFTI_INTENT_ESTIMATE => "NIFTI_INTENT_ESTIMATE"
    case NIFTI_INTENT_LABEL => "NIFTI_INTENT_LABEL"
    case NIFTI_INTENT_NEURONAME => "NIFTI_INTENT_NEURONAME"
    case NIFTI_INTENT_GENMATRIX => "NIFTI_INTENT_GENMATRIX"
    case NIFTI_INTENT_SYMMATRIX => "NIFTI_INTENT_SYMMATRIX"
    case NIFTI_INTENT_DISPVECT => "NIFTI_INTENT_DISPVECT"
    case NIFTI_INTENT_VECTOR => "NIFTI_INTENT_VECTOR"
    case NIFTI_INTENT_POINTSET => "NIFTI_INTENT_POINTSET"
    case NIFTI_INTENT_TRIANGLE => "NIFTI_INTENT_TRIANGLE"
    case NIFTI_INTENT_QUATERNION => "NIFTI_INTENT_QUATERNION"
    case _ => "INVALID_NIFTI_INTENT_CODE"
  }

  def decodeDatatype(dcode: Int): String = dcode match {
    case DT_NONE => "DT_NONE"
    case DT_BINARY => "DT_BINARY"
    case NIFTI_TYPE_UINT8 => "NIFTI_TYPE_UINT8"
    case NIFTI_TYPE_INT16 => "NIFTI_TYPE_INT16"
    case NIFTI_TYPE_INT32 => "NIFTI_TYPE_INT32"
    case NIFTI_TYPE_FLOAT32 => "NIFTI_TYPE_FLOAT32"
    case NIFTI_TYPE_COMPLEX64 => "NIFTI_TYPE_COMPLEX64"
    case NIFTI_TYPE_FLOAT64 => "NIFTI_TYPE_FLOAT64"
    case NIFTI_TYPE_RGB24 => "NIFTI_TYPE_RGB24"
    case DT_ALL => "DT_ALL"
    case NIFTI_TYPE_INT8 => "NIFTI_TYPE_INT8"
    case NIFTI_TYPE_UINT16 => "NIFTI_TYPE_UINT16"
    case NIFTI_TYPE_UINT32 => "NIFTI_TYPE_UINT32"
    case NIFTI_TYPE_INT64 => "NIFTI_TYPE_INT64"
    case NIFTI_TYPE_UINT64 => "NIFTI_TYPE_UINT64"
    case NIFTI_TYPE_FLOAT128 => "NIFTI_TYPE_FLOAT128"
    case NIFTI_TYPE_COMPLEX128 => "NIFTI_TYPE_COMPLEX128"
    case NIFTI_TYPE_COMPLEX256 => "NIFTI_TYPE_COMPLEX256"
    case NIFTI_TYPE_RGBA32 => "NIFTI_TYPE_RGBA32"
    case _ => "INVALID_NIFTI_DATATYPE_CODE"
  }

  def bytesPerVoxel(dcode: Int): Int = dcode match {
    case DT_NONE => 0
    case DT_BINARY => -(1)
    case NIFTI_TYPE_UINT8 => 1
    case NIFTI_TYPE_INT16 => 2
    case NIFTI_TYPE_INT32 => 4
    case NIFTI_TYPE_FLOAT32 => 4
    case NIFTI_TYPE_COMPLEX64 => 8
    case NIFTI_TYPE_FLOAT64 => 8
    case NIFTI_TYPE_RGB24 => 3
    case DT_ALL => 0
    case NIFTI_TYPE_INT8 => 1
    case NIFTI_TYPE_UINT16 => 2
    case NIFTI_TYPE_UINT32 => 4
    case NIFTI_TYPE_INT64 => 8
    case NIFTI_TYPE_UINT64 => 8
    case NIFTI_TYPE_FLOAT128 => 16
    case NIFTI_TYPE_COMPLEX128 => 16
    case NIFTI_TYPE_COMPLEX256 => 32
    case NIFTI_TYPE_RGBA32 => 32
    case _ => 0
  }

  def decodeSliceOrder(code: Int): String = code match {
    case NIFTI_SLICE_SEQ_INC => "NIFTI_SLICE_SEQ_INC"
    case NIFTI_SLICE_SEQ_DEC => "NIFTI_SLICE_SEQ_DEC"
    case NIFTI_SLICE_ALT_INC => "NIFTI_SLICE_ALT_INC"
    case NIFTI_SLICE_ALT_DEC => "NIFTI_SLICE_ALT_DEC"
    case _ => "INVALID_NIFTI_SLICE_SEQ_CODE"
  }

  def decodeXform(code: Int): String = code match {
    case NIFTI_XFORM_UNKNOWN => "NIFTI_XFORM_UNKNOWN"
    case NIFTI_XFORM_SCANNER_ANAT => "NIFTI_XFORM_SCANNER_ANAT"
    case NIFTI_XFORM_ALIGNED_ANAT => "NIFTI_XFORM_ALIGNED_ANAT"
    case NIFTI_XFORM_TALAIRACH => "NIFTI_XFORM_TALAIRACH"
    case NIFTI_XFORM_MNI_152 => "NIFTI_XFORM_MNI_152"
    case _ => "INVALID_NIFTI_XFORM_CODE"
  }

  def decodeUnits(code: Int): String = code match {
    case NIFTI_UNITS_UNKNOWN => "NIFTI_UNITS_UNKNOWN"
    case NIFTI_UNITS_METER => "NIFTI_UNITS_METER"
    case NIFTI_UNITS_MM => "NIFTI_UNITS_MM"
    case NIFTI_UNITS_MICRON => "NIFTI_UNITS_MICRON"
    case NIFTI_UNITS_SEC => "NIFTI_UNITS_SEC"
    case NIFTI_UNITS_MSEC => "NIFTI_UNITS_MSEC"
    case NIFTI_UNITS_USEC => "NIFTI_UNITS_USEC"
    case NIFTI_UNITS_HZ => "NIFTI_UNITS_HZ"
    case NIFTI_UNITS_PPM => "NIFTI_UNITS_PPM"
    case NIFTI_UNITS_RADS => "NIFTI_UNITS_RADS"
    case _ => "INVALID_NIFTI_UNITS_CODE"
  }

  @throws[IOException]
  private def littleEndian(stream: InputStream) = {
    if (!stream.markSupported) throw new IllegalArgumentException("stream does not support marks")
    stream.mark(42)
    val di = new DataInputStream(stream)
    di.skipBytes(40)
    val s = di.readShort
    stream.reset()
    (s < 1) || (s > 7)
  }

  /** Read a NIFTI header from a file.
    *
    * @param filename the name of the file to read the header from
    * @return
    * @throws IOException
    */
  @throws[IOException]
  def read(filename: String): NiftiHeader = {
    var is: InputStream = new FileInputStream(filename)
    if (filename.endsWith(".gz")) is = new GZIPInputStream(is)
    try
      read(is, filename)
    finally is.close()
  }

  /** Read a NIFTI header from a binary data input stream. This method assumes that the content retrieved
    * from the input stream is already uncompressed.
    *
    * @param is       a stream to read the NIFTI header from, uncompressed. This will not close the stream!
    * @param filename the original file name of the header, can be null
    * @return
    * @throws IOException
    */
  @throws[IOException]
  def read(is: InputStream, filename: String): NiftiHeader = {
    val bufferedStream =
      if (!is.isInstanceOf[BufferedInputStream]) new BufferedInputStream(is)
      else is.asInstanceOf[BufferedInputStream]
    val le = littleEndian(bufferedStream)
    val di =
      if (le) new LittleEndianDataInputStream(bufferedStream)
      else new DataInputStream(bufferedStream)
    val ds = new NiftiHeader
    ds.filename = filename
    ds.little_endian = le
    readMain(di, ds)
  }

  @throws[IOException]
  private def readMain(di: DataInput, header: NiftiHeader) = {
    header.sizeof_hdr = di.readInt
    var bb = new Array[Byte](10)
    di.readFully(bb, 0, 10)
    header.data_type_string = new String(bb).trim
    bb = new Array[Byte](18)
    di.readFully(bb, 0, 18)
    header.db_name = new String(bb).trim
    header.extents = di.readInt
    header.session_error = di.readShort
    header.regular = di.readUnsignedByte.toChar.toString
    header.dim_info = di.readUnsignedByte.toChar.toString
    val fps_dim = header.dim_info.charAt(0).toInt
    header.freq_dim = (fps_dim & 3)
    header.phase_dim = ((fps_dim >>> 2) & 3)
    header.slice_dim = ((fps_dim >>> 4) & 3)
    for (i <- 0 until 8) header.dim(i) = di.readShort
    //if (ds.dim(0) > 0) ds.dim(1) = ds.dim(1)   // what is this supposed to do ???
    //if (ds.dim(0) > 1) ds.dim(2) = ds.dim(2)   // similar in niftijio...
    //if (ds.dim(0) > 2) ds.dim(3) = ds.dim(3)
    //if (ds.dim(0) > 3) ds.dim(4) = ds.dim(4)
    for (i <- 0 until 3)  header.intent(i) = di.readFloat
    header.intent_code = di.readShort
    header.datatype = di.readShort
    header.datatype_name = NiftiHeader.decodeDatatype(header.datatype)
    header.bitpix = di.readShort
    header.slice_start = di.readShort
    for (i <- 0 until 8)  header.pixdim(i) = di.readFloat
    header.qfac = Math.floor(header.pixdim(0).toDouble).toInt
    header.vox_offset = di.readFloat
    header.scl_slope = di.readFloat
    header.scl_inter = di.readFloat
    header.slice_end = di.readShort
    header.slice_code = di.readUnsignedByte.toByte
    header.xyzt_units = di.readUnsignedByte.toByte
    val unit_codes = header.xyzt_units.toInt
    header.xyz_unit_code = (unit_codes & 7:Int)
    header.t_unit_code = (unit_codes & 56:Int) // TODO: This does not yield the expected result !!! also probably freq_dim, phase_dim and slice_dim...
    header.cal_max = di.readFloat
    header.cal_min = di.readFloat
    header.slice_duration = di.readFloat
    header.toffset = di.readFloat
    header.glmax = di.readInt
    header.glmin = di.readInt
    bb = new Array[Byte](80)
    di.readFully(bb, 0, 80)
    header.descrip = new String(bb).trim
    bb = new Array[Byte](24)
    di.readFully(bb, 0, 24)
    header.aux_file = new String(bb).trim
    header.qform_code = di.readShort
    header.sform_code = di.readShort
    for (i <- 0 until 3) header.quatern(i) = di.readFloat
    for (i <- 0 until 3) header.qoffset(i) = di.readFloat
    for (i <- 0 until 4) header.srow_x(i) = di.readFloat
    for (i <- 0 until 4) header.srow_y(i) = di.readFloat
    for (i <- 0 until 4) header.srow_z(i) = di.readFloat
    bb = new Array[Byte](16)
    di.readFully(bb, 0, 16)
    header.intent_name = new String(bb).trim
    bb = new Array[Byte](4)
    di.readFully(bb, 0, 4)
    header.magic = new String(bb).trim
    var extBytes = new Array[Byte](4) map (_ => 0:Byte)
    di.readFully(extBytes, 0, 4)
    header.extension= extBytes map {case a:Byte => a.toInt}
    if (header.extension(0) != (0:Byte)) {
      header.extension_blobs = List()
      var start_addr = ANZ_HDR_SIZE + 4
      while (start_addr < header.vox_offset.toInt) {
        val size_code = new Array[Int](2)
        size_code(0) = di.readInt
        size_code(1) = di.readInt
        val nb = size_code(0) - EXT_KEY_SIZE
        val eblob = new Array[Byte](nb)
        di.readFully(eblob, 0, nb)
        val seqBlob:Seq[Int] = eblob.toList map {case a :Byte => a.toInt}
        val seqCode:Seq[Int] = size_code.toList

        header.extension_blobs = seqBlob+:header.extension_blobs
        header.extensions_list = seqCode+:header.extensions_list
        start_addr += size_code(0)
        if (start_addr > header.vox_offset.toInt) throw new IOException("Error: Data  for extension " + header.extensions_list.size + " appears to overrun start of image data.")
      }
    }
    header
  }
}
