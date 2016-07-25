package com.zenfractal

/*Auction.scala*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.util._
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory}
import com.julianpeeters.avro.annotations._

/**
 * A class to represent auctions.
 */
 
@AvroRecord
case class Auction(var publisherId: Int, var auctionId: Long, var revenue: Float) {
	override def toString(): String = "--\npublisher: "+publisherId+"\nauction: "+auctionId+"\nrevenue: "+revenue+"\n--"
}

@AvroRecord
case class AuctionHeavy(var auctionId: Int, var publisherId: Int, var revenue: Option[Double], var publisherName: Option[String], var publisherCEOName: Option[String], var publisherCEODogName: Option[String], var publisherCEODogVetName: Option[String], var publisherCEODogVetWifeName: Option[String]) {
	override def toString(): String = "--\npublisher: "+publisherId+"\nauction: "+auctionId+"\nrevenue: "+revenue+"\n--"
}

@AvroRecord
case class AuctionFilt(var auctionId: Int, var publisherId: Int) {
  override def toString(): String = "--\npublisher: "+publisherId+"\n--"
}

object AuctionHandler{
  def randomAuction(): Auction = {
    val r = scala.util.Random
    new Auction(
      r.nextInt(100),
      r.nextInt(10000),
      r.nextFloat()*500
      )
  }

  def createAuctionSet(n: Long) : List[Auction] = {
    addAuctionSet(n, Nil)
  }

  def addAuctionSet(n: Long, l: List[Auction]) : List[Auction] = {
    if(n==0) l
    else addAuctionSet(n-1, randomAuction() :: l)
  }
}


object AuctionHeavyHandler{
  def randomAuctionHeavy(): AuctionHeavy = {
    val r = scala.util.Random
    new AuctionHeavy(
      r.nextInt(100),
      r.nextInt(10000),
      Some(r.nextDouble()*500),
      Some(randomString(250)),
      Some("azeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeazeaze" + r.nextInt(10)),
      Some("escargotescargotescargotescargotescargotescargotescargotescargotescargotescargotescargotescargotescargot" + r.nextInt(10)),
      Some("saladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesaladesalade" + r.nextInt(10)),
      Some("stringstringstring" + r.nextInt(10))
      )
  }

  def createAuctionHeavySet(n: Long) : List[AuctionHeavy] = {
    addAuctionHeavySet(n, Nil)
  }

  def addAuctionHeavySet(n: Long, l: List[AuctionHeavy]) : List[AuctionHeavy] = {
    if(n==0) l
    else addAuctionHeavySet(n-1, randomAuctionHeavy() :: l)
  }

  def randomStringRecursive(n: Int): List[Char] = {
    n match {
      case 1 => List(scala.util.Random.nextPrintableChar)
      case _ => List(scala.util.Random.nextPrintableChar) ++ randomStringRecursive(n-1)
    }
  }

  def randomString(n: Int): String = {
    randomStringRecursive(n).mkString
  }
}
