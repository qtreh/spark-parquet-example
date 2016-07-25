package com.zenfractal

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.io.Files
import com.julianpeeters.avro.annotations._
import com.twitter.chill.avro.AvroSerializer
import java.io.{File,IOException,BufferedOutputStream,FileOutputStream}
import java.lang.Iterable
import scala.collection.JavaConversions._
import org.apache.avro.generic._
import org.apache.avro.hadoop.io._
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob,AvroKeyOutputFormat,AvroKeyRecordWriter}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.serializer.{ KryoSerializer, KryoRegistrator }
import parquet.avro.{AvroParquetOutputFormat,AvroParquetInputFormat,AvroWriteSupport,AvroReadSupport}
import parquet.avro.AvroParquetInputFormat._
import parquet.column.ColumnReader
import parquet.filter.{RecordFilter,UnboundRecordFilter}
import parquet.filter.ColumnPredicates._
import parquet.filter.ColumnRecordFilter._
import parquet.format.CompressionCodec
import parquet.hadoop.{ParquetOutputFormat,ParquetInputFormat}
import parquet.hadoop.metadata.CompressionCodecName
import scala.collection.mutable.ListBuffer

// Original code : https://github.com/massie/spark-parquet-example
object SparkParquetExample {

  // This predicate will remove all amino acids that are not basic
  class BasicAuctionPredicate extends UnboundRecordFilter {
    def bind(readers: Iterable[ColumnReader]): RecordFilter = {
      column("publisherId", equalTo(13)).bind(readers)
    }
  }

  private def auctionPrinter(tuple: Tuple2[Void, Auction]) {
    if (tuple._2 != null) println(tuple._1+" --- "+tuple._2)
  }

  // Register generated Avro classes for Spark's internal serialisation
  class AuctionKryoRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      //kryo.register(classOf[Auction], AvroSerializer[Auction]())
      kryo.register(classOf[AuctionHeavy])
      // kryo.register(....)
    }
  }

  def main(args: Array[String]) {
    val nbOutliers = 5
    val nbTirages = 25
    val nbAuctions = 100000
    val nbPartitions = 16
    val x = List.fill(nbTirages)(nbAuctions)
    // x = 10000, 10000, ..., 10000

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("ParquetExample")
      .set("spark.driver.extraClassPath", "/Users/quentin/Dev/spark-1.4.1-bin-hadoop2.6/lib/parquet-avro-1.8.1.jar")
    val sc = new SparkContext(sparkConf)

    val resultsTmp = x
        .map(na => usingParquet(sc,na,nbPartitions)) // fait les tirages
        //.splitAt(nbOutliers)
        //._2 // Retire les premiers résultats (outliers)
    //println("resultsTmp: "+resultsTmp)
    val results = resultsTmp.splitAt(nbOutliers)._2
    val writingResults = results.foldLeft(List[Float]())( _ :+ _._1).sorted
    val readingResults = results.foldLeft(List[Float]())( _ :+ _._2).sorted
    val readingFilteredResults = results.foldLeft(List[Float]())( _ :+ _._3).sorted
    val readingFilteredColumnResults = results.foldLeft(List[Float]())( _ :+ _._4).sorted
    val dirSizeResults = results.foldLeft(List[Float]())( _ :+ _._5).sorted
    println("writingResults: " + writingResults)
    println("readingResults: " + readingResults)
    println("readingFilteredResults: " + readingFilteredResults)
    println("readingFilteredColumnResults: " + readingFilteredColumnResults)
    println("dirSizeResults: " + dirSizeResults)
    val writingSum = writingResults.foldLeft(0.0)(_ + _)
    val readingSum = readingResults.foldLeft(0.0)(_ + _)
    val readingFilteredSum = readingFilteredResults.foldLeft(0.0)(_ + _)
    val readingFilteredColumnSum = readingFilteredColumnResults.foldLeft(0.0)(_ + _)
    val dirSizeSum = dirSizeResults.foldLeft(0.0)(_ + _)

    val writingAvg = writingSum / (nbTirages-nbOutliers)
    val readingAvg = readingSum / (nbTirages-nbOutliers)
    val readingFilteredAvg = readingFilteredSum / (nbTirages-nbOutliers)
    val readingFilteredColumnAvg = readingFilteredColumnSum / (nbTirages-nbOutliers)
    val dirSizeAvg = dirSizeSum / (nbTirages-nbOutliers)
    val writingStd = computeStd(writingResults, writingAvg)
    val readingStd = computeStd(readingResults, readingAvg)
    val readingFilteredStd = computeStd(readingFilteredResults, readingFilteredAvg)
    val readingFilteredColumnStd = computeStd(readingFilteredColumnResults, readingFilteredColumnAvg)
    val dirSizeStd = computeStd(dirSizeResults, dirSizeAvg)

    println("writingAvg = " + writingAvg)
    println("writingStd = " + writingStd)
    println("readingAvg = " + readingAvg)
    println("readingStd = " + readingStd)
    println("readingFilteredAvg = " + readingFilteredAvg)
    println("readingFilteredStd = " + readingFilteredStd)
    println("readingFilteredColumnAvg = " + readingFilteredColumnAvg)
    println("readingFilteredColumnStd = " + readingFilteredColumnStd)
    println("dirSizeAvg = " + dirSizeAvg)
    println("dirSizeStd = " + dirSizeStd)

    val alpha = 0.9
    val quantileIndex = Math.round(alpha * (nbTirages - nbOutliers - 1))
    val writingQuantile = writingResults(quantileIndex.toInt)
    println("writingQuantile: " + writingQuantile)
    val readingQuantile = readingResults(quantileIndex.toInt)
    println("readingQuantile: " + readingQuantile)
    val readingFilteredQuantile = readingFilteredResults(quantileIndex.toInt)
    println("readingFilteredQuantile: " + readingFilteredQuantile)
    val readingFilteredColumnQuantile = readingFilteredColumnResults(quantileIndex.toInt)
    println("readingFilteredColumnQuantile: " + readingFilteredColumnQuantile)
    val dirSizeQuantile = dirSizeResults(quantileIndex.toInt)
    println("dirSizeQuantile: " + dirSizeQuantile)

    println("WRITING")
    println("READING")
    println("READING FILTERED")
    println("READING FILTERED BY COLUMN")
    println("HDFS DIRECTORY SIZE")

    println(writingAvg)
    println(readingAvg)
    println(readingFilteredAvg)
    println(readingFilteredColumnAvg)
    println(dirSizeAvg)
    println("")
    println(writingStd)
    println(readingStd)
    println(readingFilteredStd)
    println(readingFilteredColumnStd)
    println(dirSizeStd)
    println("")
    
    println(writingQuantile)
    //printToSpreadsheet(writingResults)
    println(readingQuantile)
    //printToSpreadsheet(readingResults)
    println(readingFilteredQuantile)
    //printToSpreadsheet(readingFilteredResults)
    println(readingFilteredColumnQuantile)
    //printToSpreadsheet(readingFilteredColumnResults)
    println(dirSizeQuantile)
    //printToSpreadsheet(dirSizeResults)

    println("THE END")
  }

  private def computeStd(vector: List[Float], mean: Double): Double = {
    val devs = vector.map(value => (value - mean) * (value - mean))
    Math.sqrt(devs.sum / vector.size)
  }

  private def printToSpreadsheet(listFloats: List[Float]) {
    println(listFloats.foldLeft("")( _ + _.toString + "\n"))
  }

  def usingParquet(sc: SparkContext,nbAuctions: Int, nbPartitions: Int): Tuple5[Float,Float,Float,Float,Float] = {
    val job = Job.getInstance()

    //val tempDir = Files.createTempDir()
    val outputDir = new File("outputrddfile.bin").getAbsolutePath
    println(outputDir)

    val someAuctions = AuctionHeavyHandler.createAuctionHeavySet(nbAuctions) 
    println("auction set created")
    
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    // Adds a compression
    ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP)
    // You need to pass the schema to AvroParquet when you are writing objects but not when you
    // are reading them. The schema is saved in Parquet file for future readers to use.
    AvroParquetOutputFormat.setSchema(job, AuctionHeavy.SCHEMA$)
    println("schema set")
    // Create a PairRDD with all keys set to null
    //val rdd = sc.makeRDD(someAuctions.map(auction => (null, auction)))

    val ones = List.fill(nbPartitions){1}
    val fakeRdd = sc.makeRDD(ones, nbPartitions) // 100 partitions
    val rddWithAuctions = fakeRdd.flatMap( 
        (_) => {
            AuctionHeavyHandler.createAuctionHeavySet(nbAuctions)
        }
    )
    val rdd = rddWithAuctions.map { auction => (null, auction) }
    println("partitionned rdd created !!")

    // Save the RDD to a Parquet file in our temporary output directory
    println("saving rdd...")
    val startWrite = System.currentTimeMillis()
    rdd.saveAsNewAPIHadoopFile(outputDir, classOf[Void], classOf[AuctionHeavy],
      classOf[ParquetOutputFormat[AuctionHeavy]], job.getConfiguration)
    val writingTime = System.currentTimeMillis()-startWrite
    println("Ecriture : "+writingTime+"ms")

    // Read all the amino acids back to show that they were all saved to the Parquet file
    val startRead = System.currentTimeMillis()
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AuctionHeavy]])
    val file = sc.newAPIHadoopFile(outputDir, classOf[ParquetInputFormat[AuctionHeavy]],
      classOf[Void], classOf[AuctionHeavy], job.getConfiguration)
    val countReading = file.count()
    val readingTime = System.currentTimeMillis()-startRead
    println("Lecture : "+readingTime+"ms")

    // Set a predicate and Parquet only deserializes amino acids that are basic.
    // Non-basic amino acids will returned as null.
    val startReadPredicate = System.currentTimeMillis()
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[BasicAuctionPredicate])
    val filteredFile = sc.newAPIHadoopFile(outputDir, classOf[ParquetInputFormat[AuctionHeavy]],
      classOf[Void], classOf[AuctionHeavy], job.getConfiguration)
    println("file with predicate read")
    val countReadingPred = filteredFile.count()
    val readingAndFilteringTime = System.currentTimeMillis()-startReadPredicate
    println("Lecture filtrée: "+readingAndFilteringTime+"ms")

    // Read with filter by column
    val projection = Schema.createRecord(AuctionFilt.SCHEMA$.getName(), AuctionFilt.SCHEMA$.getDoc(), AuctionFilt.SCHEMA$.getNamespace(), false)
    //projection.setFields(AuctionHeavy.SCHEMA$.getFields())
    //AvroParquetInputFormat.setRequestedProjection(job, projection)
    //AvroParquetInputFormat.setAvroReadSchema(job, AuctionFilt.SCHEMA$)

    val startReadByColumn = System.currentTimeMillis()
    AvroParquetInputFormat.setRequestedProjection(job, AuctionFilt.SCHEMA$)
    val filteredColumnFile = sc.newAPIHadoopFile(outputDir, classOf[ParquetInputFormat[AuctionHeavy]],
      classOf[Void], classOf[AuctionHeavy], job.getConfiguration)
    val countReadCol = filteredColumnFile.count()
    val readByColumnTime = System.currentTimeMillis()-startReadByColumn
    println("Lecture filtrée par colonne: "+ readByColumnTime +"ms")

    val dirSize = getDirSize(new File(outputDir))
    println("dirSize: " + dirSize)
    println("Deleting output folder...")
    FileUtils.deleteDirectory(new File(outputDir))
    println("Done")
    new Tuple5(writingTime, readingTime, readingAndFilteringTime, readByColumnTime, dirSize)
  }

  def getDirSize(directory: File): Float = {
    val lengths = for (file <- directory.listFiles()) yield {
        if (file.isFile()) file.length()
        else getDirSize(file)
    }
    lengths.sum
  }

}
