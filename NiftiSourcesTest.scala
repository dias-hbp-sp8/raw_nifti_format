package raw.executor

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import raw.tasks.{Task, Tasks}
import com.google.common.base.Stopwatch
import raw.location.LocalFile
import raw.db.Schemas
import raw.utils.RawUtils

import scala.concurrent.TimeoutException

/**
  * Created by dperez on 21.03.17.
  */


class NiftiRegisterTests extends OldStyleExecutorTest {
  val ai = new AtomicInteger()

  test("register nifti file") {
    val schemaName = genSchemaName("testNifti")
    logger.info("Registering file asynchronously")
    val testFile = LocalFile(RawUtils.toPath("data/nifti/FLOAT32_121-145-121_5d.nii"))
    val taskId = rawContext.registerStart(schemaName, testFile)
    val status = pollRegister(taskId, 5000)
    assert(status == Task.Status.Success)
    assert(Schemas.exists(Joe.user.id, schemaName))
  }

  def genSchemaName(base: String): String = {
    s"$base${ai.getAndIncrement()}"
  }

  def pollRegister(taskId: Int, maxWaitMS: Long): Task.Status = {
    val start = Stopwatch.createStarted()
    while (true) {
      Tasks.findById(taskId) match {
        case Some(task: Task) =>
          logger.info(s"Status: $task")
          task.status match {
            case Task.Status.Success | Task.Status.Failed => return task.status
            case _ =>
              val nextWait = maxWaitMS - start.elapsed(TimeUnit.MILLISECONDS)
              if (nextWait < 0) {
                throw new TimeoutException()
              }
              Thread.sleep(Math.min(500, maxWaitMS - start.elapsed(TimeUnit.MILLISECONDS)))
          }
        case _ => throw new AssertionError()
      }
    }
    null
  }
}

class TestWithFakeNIFTIFiles extends NewStyleExecutorTest {

  // The first three values of the test files should be
  // 150, 40000, 3000000000.0 and 1e19

  localFile("testNiftiINT8", "data/nifti/INT8_5-10-12.nii.gz")
  // theoretical max value: 128, so 150 will not be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiINT8") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) ==
      "List(UserRecord2(NIFTI_TYPE_INT8,8,1.0,0.0," +
        "Vector(Vector(Vector(-106.0, 64.0, 0.0, -1.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
        "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
        "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
        "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
        "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
        "Vector(60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0,")
  }

  localFile("testNiftiUINT8", "data/nifti/UINT8_5-10-12.nii.gz")
  // theoretical max value: 256, so 150 should be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiUINT8") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) ==
      "List(UserRecord2(NIFTI_TYPE_UINT8,8,1.0,0.0," +
        "Vector(Vector(Vector(150.0, 64.0, 0.0, 255.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
        "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
        "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
        "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
        "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
        "Vector(60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0")
  }

  localFile("testNiftiINT16", "data/nifti/INT16_5-10-12.nii.gz")
  // theoretical max value: 32768, so 40'000 will not be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiINT16") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_INT16,16,1.0,0.0," +
      "Vector(Vector(Vector(150.0, -25536.0, 24064.0, -1.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 64.0, 65")
  }

  localFile("testNiftiUINT16", "data/nifti/UINT16_5-10-12.nii.gz")
  // theoretical max value: 65536, so 40'000 should be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiUINT16") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_UINT16,16,1.0,0.0," +
      "Vector(Vector(Vector(150.0, 40000.0, 24064.0, 65535.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 64.0,")
  }

  localFile("testNiftiINT32", "data/nifti/INT32_5-10-12.nii.gz")
  // theoretical max value: 2'147'483'648, so 3'000'000'000 will not be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiINT32") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_INT32,32,1.0,32.0," +
      "Vector(Vector(Vector(150.0, 40000.0, -1.2949673E9, 31.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 64.")
  }

  localFile("testNiftiUINT32", "data/nifti/UINT32_5-10-12.nii.gz")
  // theoretical max value: 4'294'967'296, so 3'000'000'000 should be ok...
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiUINT32") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_UINT32,32,1.0,-32.0," +
      "Vector(Vector(Vector(150.0, 40000.0, 3.0E9, 4.2949673E9, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 6")
  }

  localFile("testNiftiINT64", "data/nifti/INT64_5-10-12.nii.gz")
  // theoretical max value: 2^63 = 9.22337e18, so 1e19 will not be ok...
  // Actually 1e19 was rounded to the long max value (9.22337e18) at writing time, and it is read correctly
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiINT64") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_INT64,64,1.0,-6.0," +
      "Vector(Vector(Vector(150.0, 40000.0, 3.0E9, 9.223372E18, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 64.")
  }

  /*  localFile("testNiftiUINT64", "data/nifti/UINT64_5-10-12.nii.gz")
    test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiUINT64") { x =>
      val res = executeQuery(x.q)
      println("Result ?  " + res.toString.substring(0,500))
      assert(res.toString.substring(0,500) == "List(UserRecord2(16,32,352.0,2,8))")
    }*/

  localFile("testNiftiFLOAT32", "data/nifti/FLOAT32_5-10-12.nii.gz")
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiFLOAT32") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_FLOAT32,32,3.0,-8.0," +
      "Vector(Vector(Vector(150.0, 40000.0, 3.0E9, 1.0E19, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0), " +
      "Vector(12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0), " +
      "Vector(24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0), " +
      "Vector(36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0), " +
      "Vector(48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0), " +
      "Vector(60.0, 61.0, 62.0, 63.0, 64.0, ")
  }

  /*localFile("testNiftiFLOAT64", "data/nifti/FLOAT64_5-10-12.nii.gz")
  test("SELECT header.datatype_name, header.bitpix, header.scl_slope, header.scl_inter, data FROM testNiftiFLOAT64") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(UserRecord2(NIFTI_TYPE_FLOAT64,32,0.5,-2.0," +
      "Vector(Vector(Vector(-2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0), " +
      "Vector(-0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125), " +
      "Vector(-2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.0), " +
      "Vector(-0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125, -0.8125), " +
      "Vector(-2.0, -2.0, -2.0, -2.0, -2.0, -2.0, -2.")
  }*/
}


class TestsWithRealNIFTIFiles extends NewStyleExecutorTest {

  localFile("testNifti1", "data/nifti/FLOAT32_121-145-121_5d.nii")

  test("select header.dim from testNifti1") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString )
    //true // should run first, let's see later what the result is supposed to be...
  }

  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.xyz_unit_code, header.t_unit_code FROM testNifti1") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord2(16,32,352.0,2,8))")
  }

  test("SELECT data FROM testNifti1") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,500))
    assert(res.toString.substring(0,500) == "List(Vector(Vector(Vector(" +
      "Vector(Vector(-0.5468917, -0.38190025, -0.6130992)), " +
      "Vector(Vector(-0.5474835, -0.38226134, -0.61271423)), " +
      "Vector(Vector(-0.5486672, -0.38298124, -0.6119447)), " +
      "Vector(Vector(-0.5504426, -0.3840565, -0.6107918)), " +
      "Vector(Vector(-0.552808, -0.3854815, -0.6092566)), " +
      "Vector(Vector(-0.55576044, -0.3872487, -0.6073408)), " +
      "Vector(Vector(-0.5592955, -0.38934854, -0.60504586)), " +
      "Vector(Vector(-0.5634067, -0.39176962, -0.6023737)), " +
      "Vector(Vector(-0.56808627, -0.39449865, -0.5993265)")
  }

  localFile("testNifti2", "data/nifti/FLOAT32_256-256-160.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.scl_slope, header.scl_inter, header.qform_code FROM testNifti2") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord4(16,32,352.0,1.0,0.0,1))")
  }
  test("select data from testNifti2") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,2000))
    assert(res.toString.substring(0,2000)=="List(Vector(Vector(Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0")
  }

  localFile("testNifti3", "data/nifti/INT16_64-64-32_4d.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.xyz_unit_code, header.t_unit_code FROM testNifti3") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord2(4,16,352.0,2,8))")
  }

  localFile("testNifti4", "data/nifti/INT16_96-96-80.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.scl_slope, header.scl_inter, header.qform_code FROM testNifti4") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord4(4,16,352.0,4.4947495,0.0,1))")
  }
  test("select data from testNifti4") { x =>
    val res = executeQuery(x.q)
    //println("Result ?  " + res.toString.substring(0,2000))
    assert(res.toString.substring(0,2000)=="List(Vector(Vector(Vector(220.24272, 143.83199, 58.431744, 125.85299, 229.23222, 94.38974, 62.926495, 188.77948, 80.905495, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 49.442245, 4.4947495, 229.23222, 287.66397, 31.463247, 44.947495, 35.957996, 26.968498, 67.42124, 148.32674, 346.0957, 107.87399, 202.26373, 67.42124, 13.484249, 40.452747), Vector(247.21123, 71.91599, 80.905495, 125.85299, 58.431744, 310.13773, 130.34773, 256.2007, 125.85299, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 215.74799, 148.32674, 76.41074, 107.87399, 184.28473, 112.36874, 471.9487, 206.75848, 341.60095, 206.75848, 170.80048, 58.431744, 148.32674, 103.37924, 170.80048, 166.30574), Vector(220.24272, 166.30574, 247.21123, 112.36874, 256.2007, 337.1062, 103.37924, 184.28473, 80.905495, 125.85299, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 193.27423, 170.80048, 130.34773, 35.957996, 58.431744, 251.70598, 58.431744, 211.25323, 215.74799, 62.926495, 161.81099, 139.33723, 89.89499, 202.26373, 98.88449, 98.88449, 89.89499), Vector(53.936996, 94.38974, 98.88449, 98.88449, 188.77948, 53.936996, 256.2007, 103.37924, 44.947495, 238.22173, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0")
  }

  localFile("testNifti5", "data/nifti/INT16_240-256-160.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.xyz_unit_code, header.t_unit_code FROM testNifti5") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord2(4,16,352.0,2,0))")
  }

  localFile("testNifti6", "data/nifti/UINT8_240-256-160.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.scl_slope, header.scl_inter, header.qform_code FROM testNifti6") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord4(2,8,352.0,0.003921569,0.0,2))")
  }
  test("select data from testNifti6") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,2000))
    assert(res.toString.substring(0,2000)=="List(Vector(Vector(Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0")
  }

  localFile("testNifti7", "data/nifti/UINT8_240-256-176.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.xyz_unit_code, header.t_unit_code FROM testNifti7") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord2(2,8,352.0,2,8))")
  }

  localFile("testNifti8", "data/nifti/UINT16_240-256-176.nii")
  test("SELECT header.datatype, header.bitpix, header.vox_offset, header.scl_slope, header.scl_inter, header.qform_code FROM testNifti8") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString)
    assert(res.toString == "List(UserRecord4(512,16,352.0,1.0,0.0,1))")
  }
  test("select data from testNifti8") { x =>
    val res = executeQuery(x.q)
    println("Result ?  " + res.toString.substring(0,2000))
    assert(res.toString.substring(0,2000)=="List(Vector(Vector(Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0")
  }

}
