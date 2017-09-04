package raw.runtime.inputformats.nifti


/**
  * Created by dperez on 22.03.17.
  */

import java.io.{DataInput, DataInputStream, IOException, InputStream}
import java.util.zip.GZIPInputStream

import com.google.common.io.LittleEndianDataInputStream
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import raw.inputformats.nifti.NiftiHeader


abstract class NiftiRecordParser[+T] {
  def parse(hdr: NiftiHeader, di: DataInput): T
}

/**
  * NiftiRecordReader reads a Nifti file
  * It does not iterate over records, as there is only one data element per Nifti file
  * @param parser
  * @tparam T
  */
final class NiftiRecordReader[T](parser: NiftiRecordParser[T]) extends RecordReader[NullWritable,T] with StrictLogging {
  private[this] var is: InputStream = _
  private[this] var path: Path = _
  private[this] var currentValue: T = _
  private[this] var notReadYet: T = _
  private[this] var niftiHeader: NiftiHeader = _
  private[this] var context: TaskAttemptContext = _

  override def initialize(inputSplit: InputSplit, contxt: TaskAttemptContext): Unit = {
    context = contxt
    val split = inputSplit.asInstanceOf[FileSplit]
    path = split.getPath  // the other inputSplit fields are not used ?
    logger.info(s"Opening file: $path")

    val fs: FileSystem = path.getFileSystem(context.getConfiguration)
    is = fs.open(path)
    if (path.toUri.getPath.endsWith(".gz")) is = new GZIPInputStream(is)

    niftiHeader = readHeader(is)

    /*
     * DP: This code follows the way JsonRecordReader opens the necessary InputStream
     * Not sure why this is so different from the way the file is open for the Inferrer:
     *
     * val is = new LocationUtils(client, settings).getInputStream(location)
     *
     * (LocationUtils is not available in this module.)
     *
     * Getting a fileSplit as parameter probably does not make much sense for NIFTI files,
     * but this seems to be hadoop's way of doing things (RecordReader is a hadoop class).
     * Are there alternatives?
     *
     */

  }

  @throws[IOException]
  def readHeader(is: InputStream): NiftiHeader = {
    try
      NiftiHeader.read(is, path.toUri.getPath)
    finally is.close()
  }

  override def getCurrentKey: NullWritable = NullWritable.get()

  // Read the next key/value
  override def nextKeyValue(): Boolean = {
    if (currentValue == notReadYet) {
      // Get right file to read data (might be another file)
      // TODO This might fail if the data source is not a file. Way to test that?
      // TODO Test with two-files nifti.
      val volumeFilePath = {
        if (niftiHeader.magic == "ni1") { // image data is stored in the .img file corresponding to the header file (offset 0)
          val stringPath = path.toUri.getPath
          new Path(stringPath.substring(0, stringPath.lastIndexOf('.') + 1) + "img")
        }
        else
          path
      }

      // Open input stream, unzip if necessary
      val fs: FileSystem = volumeFilePath.getFileSystem(context.getConfiguration)
      is = fs.open(volumeFilePath)
      if (volumeFilePath.toUri.getPath.endsWith(".gz")) is = new GZIPInputStream(is)

      try {
        // Skip offset
        is.skip(niftiHeader.vox_offset.toLong)

        // Deal with endian by using a LEDataInputStream if necessary.
        // Removed step of creating a buffered stream -> is it ok ?
        // I.e. was it just done to check endian before reading header in NiftiHeader / read header in NiftiVolume ?
        val di =
        if (niftiHeader.little_endian) new LittleEndianDataInputStream(is)
        else new DataInputStream(is)

        logger.info(s"Nifti volume file: $volumeFilePath")
        currentValue = parser.parse(niftiHeader, di)

        true
      }
      finally is.close()
    }
    else false
  }

  override def close(): Unit = {
    if (is != null) {
      is.close()
    }
  }

  override def getCurrentValue: T = currentValue

  // FIXME: We should return the current position in the file, but we need to know the size of the
  // file plus the current position. It doesn't look to be important.
  override def getProgress: Float = 0.5f
}
