package utils

import scala.collection.mutable._
import scala.io._
import scala.math._

import java.util.{Calendar, Date, TreeMap}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class VantagePoint(
  val regionNo: Int,
  val networkLocation: String,
  val city: String,
  val state: String,
  val GeoCode: String
)

case class Target(
  val targetIP: String,
  val GeoCode: String,
  val asNos: Array[Int]
)

case class Trace(
  val timestamp: Long,
  val regionNo: String,
  val targetIP: String,
  val hops: List[Long],
  val lats: List[Double],
  val numHops: Int,
  val e2eLatency: Double
)

case class GeoAS_Trace(
  val timestamp: Long,
  val fromIP: String,
  val from: String,
  val to: String,
  val hops: List[String], // AS or Geo hops (NOT IP hops)
  val segmentLats: List[Double], // AS1, AS1-AS2, AS2, AS2-AS3, ...
  val hopIndices: List[Int],
  val numHops: Int,
  val e2eLatency: Double,
  val hasNegative: Boolean,
  val inASLat: Double
)

case class IP2CAS_Mapping(
  val ipRange: Long,
  val GeoCode: String,
  val asNo: String
)

object Dataset extends Enumeration {
  type Dataset = Value
  val LARGECDN, ARK, IPLANE = Value
}
import Dataset._

object LineReaders extends Serializable {
  
  /**
   * Returns: a Trace object from a line of trace. 
   * Different Dataset has different line format; hence, different treatment
   */
  def newTraceWithHops(line: String, dataset: Dataset): Option[Trace] = {
    dataset match {
      case LARGECDN => {
        val y = line.split(",")

        val timestamp = y(2).toLong * 1000L

        val z = y(5).split("\\|")
        val numHops = z.size
        val hops = Array.fill[Long](numHops)(0)
        val lats = Array.fill[Double](numHops)(0.0)
        for (i <- 0 until numHops) {
          val p = z(i).split("[:_]")
          val latency = math.round(p.last.toDouble)
          hops(i) = Utils.ipToLong(p(1))
          lats(i) = latency
        }
        Some(Trace(timestamp, y(3), y(4), hops.toList, lats.toList, numHops, lats.last))
      }
      case ARK => {
        val y = line.split("\\s+")

        // Only taking the month
        val timestamp = Utils.timestampFromMonth(y(0).take(6))

        val numHops = (y.length - 4) / 2
        val hops = Array.fill[Long](numHops)(0)
        val lats = Array.fill[Double](numHops)(0.0)
        for (i <- 0 until numHops) {
          hops(i) = Utils.ipToLong(y(2 * i + 4))
          lats(i) = y(2 * i + 5).toDouble
        }
        Some(Trace(timestamp, y(1), y(2), hops.toList, lats.toList, numHops, lats.last))
      }
      case IPLANE => {
        val y = line.split("\\s+")

        // Only taking the month
        val timestamp = Utils.timestampFromMonth(y(0)take(6))

        val numHops = y.length / 2
        val hops = Array.fill[Long](numHops)(0)
        val lats = Array.fill[Double](numHops)(0.0)
        for (i <- 0 until numHops) {
          hops(i) = Utils.ipToLong(y(2 * i + 1))
          lats(i) = y(2 * i + 2).toDouble
        }
        Some(Trace(timestamp, y(1), y(y.length - 2), hops.toList, lats.toList, numHops, lats.last))
      }
    }
  }

  /**
   * Returns: a Trace object without actual hops.
   */
  def newTraceNoHops(line: String, dataset: Dataset): Option[Trace] = {
    val trace:Trace = newTraceWithHops(line, dataset) match {
                        case Some(x) => x
                        case _ => return None
                      }
    Some(Trace(trace.timestamp, trace.regionNo, trace.targetIP, null, null, trace.numHops, trace.e2eLatency))
  }

  /**
   * Returns: a TreeMap containing IP_Range->IP2CAS_Mapping map.
   */
  def ipEdgescapeTreeMapNew(sc: SparkContext, pathToEdgescapeFile: String): TreeMap[Long, IP2CAS_Mapping] = {
    val eLines = Source.fromFile(pathToEdgescapeFile).getLines.toList
    val ipRanges2 = eLines.map(newIP2CAS_MappingNew(_))
    val ret = new TreeMap[Long, IP2CAS_Mapping]
    for (ipr <- ipRanges2)
      ret.put(ipr.ipRange, IP2CAS_Mapping(ipr.ipRange, ipr.GeoCode, ipr.asNo))
    ret
  }

  /**
   * Returns: a GeoAS_Trace from a trace line after appropriate conversion
   * The intermediate hops are geolocations, while the endpoints can be AS or geolocation based
   * on epIsGeo value (true is geo)
   */
  def newTraceDetailed_GeoLevel(line: String, dataset: Dataset, ipMap: TreeMap[Long, IP2CAS_Mapping], epIsGeo: Boolean = true): Option[GeoAS_Trace] = {
    val trace:Trace = newTraceWithHops(line, dataset) match {
                        case Some(x) => x
                        case _ => return None
                      }
    val hasNegative = trace.lats.filter(_ < 0.0).size > 0
  
    // Compress to Geo level
    val cHops = new ArrayBuffer[String]
    val segmentLats = new ArrayBuffer[Double]
    var lastSegmentTS = 0.0
    val hopIndices = new ArrayBuffer[Int]
    cHops += ipMap.get(ipMap.floorKey(trace.hops(0))).GeoCode
    var numCHops = 1
    for (i <- 1 until trace.numHops) {
      val c = ipMap.get(ipMap.floorKey(trace.hops(i))).GeoCode
      if (c != cHops(numCHops - 1)) {
        cHops += c
        hopIndices += i

        // Time inside last AS
        segmentLats += (trace.lats(i - 1) - lastSegmentTS)
        // Time on the inter-AS link
        segmentLats += (trace.lats(i) - trace.lats(i - 1))
        // Remember
        lastSegmentTS = trace.lats(i)

        numCHops += 1
      }
    }
    // Add an extra one for future calculation
    hopIndices += trace.numHops
    // Add last segment
    segmentLats += (trace.e2eLatency - lastSegmentTS)

    // Calculate time spent inside countries
    var inGeoLat = 0.0
    var i = 0
    for (i <- 0 until segmentLats.length) {
      if (i % 2 == 0)
        inGeoLat += segmentLats(i)
    }

    val (srcEP, dstEP) = {
      if (epIsGeo) {
        (ipMap.get(ipMap.floorKey(trace.hops.head)).GeoCode, ipMap.get(ipMap.floorKey(trace.hops.last)).GeoCode)
      }
      else {
        (ipMap.get(ipMap.floorKey(trace.hops.head)).asNo, ipMap.get(ipMap.floorKey(trace.hops.last)).asNo) 
      }
    }

    Some(GeoAS_Trace(trace.timestamp, 
                         trace.regionNo,
                         srcEP, 
                         dstEP,
                         cHops.toList, 
                         segmentLats.toList, 
                         hopIndices.toList, 
                         numCHops,
                         trace.e2eLatency,
                         hasNegative,
                         100.0 * inGeoLat / trace.e2eLatency))
  }

  /**
   * Returns: a GeoAS_Trace from a trace line after appropriate conversion
   * The intermediate hops are ASes, while the endpoints can be AS or geolocation based
   * on epIsGeo value (true is geo)
   */
  def newTraceDetailed_GeoASLevel(line: String, dataset: Dataset, ipMap: TreeMap[Long, IP2CAS_Mapping], epIsGeo: Boolean): Option[GeoAS_Trace] = {
    val trace:Trace = LineReaders.newTraceWithHops(line, dataset) match {
                        case Some(x) => x
                        case _ => return None
                      }
    val hasNegative = trace.lats.filter(_ < 0.0).size > 0

    // Compress to AS level
    val asHops = new ArrayBuffer[String]
    val segmentLats = new ArrayBuffer[Double]
    var lastSegmentTS = 0.0
    val hopIndices = new ArrayBuffer[Int]
    var numAsHops = 1
    asHops += ipMap.get(ipMap.floorKey(trace.hops(0))).asNo
    hopIndices += 0
    for (i <- 1 until trace.numHops) {
      val as = ipMap.get(ipMap.floorKey(trace.hops(i))).asNo
      if (as != asHops(numAsHops - 1)) {
        asHops += as
        hopIndices += i

        // Time inside last AS
        segmentLats += (trace.lats(i - 1) - lastSegmentTS)
        // Time on the inter-AS link
        segmentLats += (trace.lats(i) - trace.lats(i - 1))
        // Remember
        lastSegmentTS = trace.lats(i)

        numAsHops += 1
      }
    }
    // Add an extra one for future calculation
    hopIndices += trace.numHops
    // Add last segment
    segmentLats += (trace.e2eLatency - lastSegmentTS)

    // Calculate time spent inside ASes
    var inASLat = 0.0
    var i = 0
    for (i <- 0 until segmentLats.length) {
      if (i % 2 == 0)
        inASLat += segmentLats(i)
    }

    val (srcEP, dstEP) = {
      if (epIsGeo) {
        (ipMap.get(ipMap.floorKey(trace.hops.head)).GeoCode, ipMap.get(ipMap.floorKey(trace.hops.last)).GeoCode)
      }
      else {
        (ipMap.get(ipMap.floorKey(trace.hops.head)).asNo, ipMap.get(ipMap.floorKey(trace.hops.last)).asNo) 
      }
    }

    Some(GeoAS_Trace(trace.timestamp, 
                         trace.regionNo, // srcIP
                         srcEP, 
                         dstEP,
                         asHops.toList, 
                         segmentLats.toList, 
                         hopIndices.toList, 
                         numAsHops,
                         trace.e2eLatency,
                         hasNegative,
                         100.0 * inASLat / trace.e2eLatency))
  }

  /**
   * Returns: a IP2CAS_Mapping object by parsing the line of mapping data
   * line's format: <IP_Range> <AS> <GeoCode> [City]
   */ 
  def newIP2CAS_MappingNew(line: String): IP2CAS_Mapping = {
    val y = line.split("\\s+")
    val ipRange = y(0).toLong
    val asNos = y(1)
    var GeoCode = {
      if (y.length > 3)
        y(3).toUpperCase + "@" + y(2).toUpperCase
      else
        y(2).toUpperCase
    }
    IP2CAS_Mapping(ipRange, GeoCode, asNos)
  }

}
