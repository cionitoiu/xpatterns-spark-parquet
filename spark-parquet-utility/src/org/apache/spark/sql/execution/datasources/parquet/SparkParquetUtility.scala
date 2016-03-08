package org.apache.spark.sql.execution.datasources.parquet

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit, Job}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{ScalaReflection, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericMutableRow}
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, BlockMetaData, FileMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Adds Parquet utilities for Jaws at the package level that allows us to use Spark internal logic.
  */
object SparkParquetUtility extends SparkHadoopMapReduceUtil with Serializable {

  implicit val defaultParquetBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE
  implicit val defaultCodec = CompressionCodecName.UNCOMPRESSED

  /**
    * Reads Parquet metadata and returns its schema as DataFrame.
    * @param sqlC - sparl sql context
    */
  implicit class xPatternsSQL(sqlC: SQLContext) {

    def readXPatternsParquet(nameNodeUrl: String, path: String) = {

      val nameNode = sanitizePath(nameNodeUrl)
      val finalPath = sanitizePath(path)
      val sc = sqlC.sparkContext

      val metadata = readMetadata(sc, nameNode, finalPath)
      val inputSplitsList = getInputSplits(sc, nameNode, finalPath, metadata)
      val inputSplitsRdd = sc.parallelize(inputSplitsList, inputSplitsList.size)
      val id = inputSplitsRdd.id

      val rdd = inputSplitsRdd.flatMap(tuplePath => {
        val (conf, _, inputformat) = initializeJob(metadata, nameNode)

        val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, 0, 0)
        val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)

        val reader = inputformat.createRecordReader(tuplePath.t, hadoopAttemptContext)
        reader.initialize(tuplePath.t, hadoopAttemptContext)

        val partitionList = ListBuffer[InternalRow]()
        while (reader.nextKeyValue) {
          partitionList. += (reader.getCurrentValue.copy)
        }
        
        partitionList
      })

      val df = sqlC.internalCreateDataFrame(rdd, DataType.fromJson(metadata).asInstanceOf[StructType])
      df
    }
  }

  implicit class xPatternsRDD[T <: Product: TypeTag: ClassTag](rdd: RDD[T]) {

    def saveAsXPatternsParquet(nameNodeUrl: String,
                               folderPath: String,
                               codec: CompressionCodecName = defaultCodec,
                               parquetBlockSize: Int = defaultParquetBlockSize) = {

      val nameNode = sanitizePath(nameNodeUrl)
      val path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = getAttributesList[T]

      writeMetadata(rdd.context, nameNode, attributes, finalWritePath)
      val errorRDD = rdd.mapPartitionsWithIndex((index, iterator) => {

        val errorList = scala.collection.mutable.MutableList[T]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet",
                                             attributes,
                                             parquetBlockSize = parquetBlockSize,
                                             codec = codec)

        while (iterator.hasNext) {
          val objectToWrite = iterator.next()
          try {
            parquetWriter.write(transformObjectToRow(objectToWrite))
          } catch {
            case e: Exception => errorList += objectToWrite
          }
        }

        parquetWriter.close()
        errorList.toList.iterator
      })

      (errorRDD.count(), errorRDD)
    }

    private def transformObjectToRow[A <: Product](data: A): InternalRow = {
      val mutableRow = new GenericMutableRow(data.productArity)
      var i = 0
      while (i < mutableRow.numFields) {
        mutableRow(i) = CatalystTypeConverters.convertToCatalyst(data.productElement(i))
        i += 1
      }
      mutableRow
    }

    private def getAttributesList[T <: Product: TypeTag]: Seq[Attribute] = {
      val att = ScalaReflection.attributesFor[T]
      println("Atributes from schema: " + att.map { x => x + ";" })
      att
    }
  }

  implicit class xPatternsDataFrame(df: DataFrame) {

    def saveAsXPatternsParquet(nameNodeUrl: String,
                               folderPath: String,
                               codec: CompressionCodecName = defaultCodec,
                               parquetBlockSize: Int = defaultParquetBlockSize) = {
      val nameNode = sanitizePath(nameNodeUrl)
      val path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = df.schema.toAttributes
      println(attributes)

      writeMetadata(df.sqlContext.sparkContext, nameNode, attributes, finalWritePath)
      val errorRDD = df.map(r => r).mapPartitionsWithIndex((index, iterator) => {
        val errorList = scala.collection.mutable.MutableList[InternalRow]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet",
                                             attributes,
                                             parquetBlockSize = parquetBlockSize,
                                             codec = codec)

        while (iterator.hasNext) {
          val objectToWrite = InternalRow.fromSeq(iterator.next().toSeq)
          try {
            parquetWriter.write(objectToWrite)
          } catch {
            case e: Exception => errorList += objectToWrite
          }
        }

        parquetWriter.close()
        errorList.toList.iterator
      })

      (errorRDD.count, errorRDD)
    }
  }

  /**
    * Obtains a Parquet writer object based on parameters below
    * @param filePath    - path to file
    * @param attributes  - schema attributes
    * @param codec       - compression codec (default value)
    * @param parquetBlockSize  -- default value
    * @return
    */
  def getParquetWriter(filePath: String,
                       attributes: Seq[Attribute],
                       codec: CompressionCodecName = defaultCodec,
                       parquetBlockSize: Int = defaultParquetBlockSize): ParquetWriter[InternalRow] = {

    val conf = new Configuration()
    val writeSupport = new CatalystWriteSupport
    CatalystWriteSupport.setSchema(StructType.fromAttributes(attributes), conf)

    new ParquetWriter[InternalRow](new Path(filePath),
                                   writeSupport,
                                   codec,
                                   parquetBlockSize,
                                   ParquetWriter.DEFAULT_PAGE_SIZE,
                                   ParquetWriter.DEFAULT_PAGE_SIZE,
                                   ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                                   ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                                   ParquetWriter.DEFAULT_WRITER_VERSION,
                                   conf)
  }

  /**
    * Writes metadata file to Parquet.
    * @param sc          -- current spark context
    * @param nameNode    -- hadoop name node address
    * @param attributes  -- schema attributes
    * @param folderPath  -- destination folder
    */
  def writeMetadata(sc: SparkContext, nameNode: String, attributes: Seq[Attribute], folderPath: String): Unit = {
    val metdataRdd = sc.parallelize(List(folderPath), 1)
    metdataRdd.foreach { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      val schema = StructType.fromAttributes(attributes)
      val parquetSchema = new CatalystSchemaConverter().convert(schema)
      val extraMetadata = mapAsJavaMap(Map(CatalystReadSupport.SPARK_METADATA_KEY -> schema.json))
      val createdBy = s"Apache Spark ${org.apache.spark.SPARK_VERSION}"
      val fileMetadata = new FileMetaData(parquetSchema, extraMetadata, createdBy)
      val parquetMetadata = new ParquetMetadata(fileMetadata, bufferAsJavaList(Seq.empty[BlockMetaData].toBuffer))
      val fpath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
      val footer = new Footer(fpath, parquetMetadata)
      ParquetFileWriter.writeMetadataFile(conf, fpath, bufferAsJavaList(Seq(footer).toBuffer))
    }
  }

  /**
    * Reads metadata schema from Parquet file via file footer.
    * @param sc           -- spark context
    * @param nameNode     -- hadoop name node address
    * @param folderPath   -- source folder
    * @return
    */
  def readMetadata(sc: SparkContext, nameNode: String, folderPath: String) = {
    val metdataRdd = sc.parallelize(List(folderPath), 1)
    metdataRdd.map { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      val footer = ParquetFileReader.readFooter(conf,
                                                new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE),
                                                ParquetMetadataConverter.NO_FILTER)
      val st = ParquetRelation.readSchemaFromFooter(new Footer(new Path(path),footer), new CatalystSchemaConverter())
      st.json
    }.first
  }

  def getInputSplits(sc: SparkContext, nameNodeUrl: String, path: String, metadata: String) = {
    val parquetFolderPath = nameNodeUrl + path
    val parquetFolderRddPath: RDD[String] = sc.parallelize(List(parquetFolderPath), 1)

    val rdd = parquetFolderRddPath.flatMap(path => {
      val fs = FileSystem.newInstance(URI.create(nameNodeUrl), new Configuration())
      val filesList = fs.listFiles(new Path(path), false)
      var pathListResult = List[String]()
      val (_, job, inputformat) = initializeJob(metadata, nameNodeUrl)

      while (filesList.hasNext) {
        val fileName = filesList.next().getPath.toString
        if (fileName.endsWith(".parquet")) {
          pathListResult = pathListResult ::: List(fileName)
          addInputPath(job, new Path(fileName))
        }
      }

      fs.close()
      inputformat.getSplits(job).map(inputSplit => {
        new SerializableWritable(inputSplit.asInstanceOf[InputSplit with Writable])
      })
    })

    rdd.collect.toList
  }

  def sanitizePath(path: String) = {
    val pathIntermediary = if (path.endsWith("/")) path.substring(0, path.length - 1) else path
    pathIntermediary
  }

  def addInputPath(job: Job, path: Path) = {
    val conf = job.getConfiguration
    val dirStr = StringUtils.escapeString(path.toString)
    val dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, if (dirs == null) dirStr else dirs + "," + dirStr)
  }

  val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  def initializeJob(metadata: String, namenode: String): (Configuration, Job, ParquetInputFormat[InternalRow]) = {
    val jobConf = SparkHadoopUtil.get.newConfiguration(new SparkConf())

    val job = Job.getInstance(jobConf)
    ParquetInputFormat.setReadSupportClass(job, classOf[CatalystReadSupport])

    val conf: Configuration = ContextUtil.getConfiguration(job)
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, namenode)
    conf.set(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA, metadata)

    val inputFormat = classOf[ParquetInputFormat[InternalRow]].newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }

    (conf, job, inputFormat)
  }
}