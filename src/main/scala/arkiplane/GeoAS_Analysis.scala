package arkiplane

import utils._
import utils.Dataset._

import scala.collection.mutable._
import scala.concurrent.duration._
import scala.math._

import java.io._
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object GeoAS_Analysis {

  private def getFilename(prefix: String, name: String, suffix: String) = {
    prefix + "-" + name + "." + suffix
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: GeoAS_Analysis <ip-X-X-X-X> <ARK|IPLANE>")
      System.exit(1)
    }

    val ipx =  args(0)
    val SPARK_PATH = "spark://" + ipx + ":7077"
    val HDFS_PATH = "hdfs://" + ipx + ":9010/"

    val DATASET_NAME = args(1).toUpperCase() match {
      case "ARK" => ARK
      case "IPLANE" => IPLANE
    }

    val cityOrGeo = "City".toUpperCase()
    val FN_SUFFIX = {
      if (cityOrGeo == "CITY")
        "CityAS"
      else
        "GeoAS"
    }
    val MAPPING_SUFFIX = {
      if (cityOrGeo == "CITY")
        "-City"
      else
        ""
    }
    
    // Setup system properties
    System.setProperty("spark.storage.blockManagerTimeoutIntervalMs", "300000")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.worker.timeout", "300")
    System.setProperty("spark.executor.memory", "56G")
    System.setProperty("spark.akka.frameSize", "1024")
    
    val sc = new SparkContext(SPARK_PATH, "GeoAS_Analysis_ARKIPLANE", "/root/spark", Seq("/root/bigtrace/target/scala-2.9.3/bigtrace_2.9.3-0.0.jar"))

    // Create IP-to-AS mapping    
    val PATH_TO_MAPPING_FILE = ""
    val ipMap = LineReaders.ipEdgescapeTreeMapNew(sc, PATH_TO_MAPPING_FILE)
    val ipMap_bc = sc.broadcast(ipMap)
    
    var traceLines = sc.textFile(HDFS_PATH + "/" + DATASET_NAME)

    // Preprocessing for ARK (Keep those without error codes)
    if (DATASET_NAME == ARK) {
      traceLines = traceLines.filter(_.contains("MOS"))
    }

    // Remove 0.0.0.0
    traceLines = traceLines.filter(x => !x.contains("0.0.0.0") && !x.contains("-"))

    val GeoASTracesAll = traceLines.flatMap(LineReaders.newTraceDetailed_GeoASLevel(_, DATASET_NAME, ipMap_bc.value, true)).cache

    for (year <- List("2008", "2009", "2010", "2011", "2012", "2013")) {
      val GeoASTraces = {
        if (year == "ALL")
          GeoASTracesAll
        else
          GeoASTracesAll.filter(x => Utils.monthFromTimestamp(x.timestamp).contains(year))
      }
      val FN_PREFIX = DATASET_NAME + "-" + year

      /*************************
       * Groupby Src-Dst Pairs *
       *************************/

      val pairs = GeoASTraces.groupBy(x => (x.fromIP, x.to))//.filter(_._2.size >= 12)
      val pairStats = pairs.map(x => {
          val y = x._2.map(_.e2eLatency).toArray
          val z = x._2.map(_.numHops.toDouble).toArray
          val a = x._2.map(_.hopIndices.last.toDouble).toArray
          (x._1, y.length, 
            Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y),
            Utils.min(z), Utils.max(z), Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z), Utils.percentiles(z),
            Utils.min(a), Utils.max(a), Utils.average(a), Utils.stdev(a), Utils.coeffOfVar(a), Utils.percentiles(a),
            (0.0, 0.0, 0.0)
            )
        })
      pairStats.saveAsTextFile(HDFS_PATH + "/" + getFilename(FN_PREFIX, "pairStats", FN_SUFFIX))

      /*******************
       * Groupby Sources *
       *******************/
      val srcs = GeoASTraces.groupBy(_.from)
      val srcsStats = srcs.map(x => {
        val y = x._2.map(_.e2eLatency).toArray
        val z = x._2.map(_.numHops.toDouble).toArray
        (x._1, y.length, 
          Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), 
          Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z))
      }).collect
      Utils.writeToFile(srcsStats, getFilename(FN_PREFIX, "srcsStats", FN_SUFFIX))
      
      /************************
       * Groupby Destinations *
       ************************/
      val dsts = GeoASTraces.groupBy(_.to).cache
      val dstsStats = dsts.map(x => {
        val y = x._2.map(_.e2eLatency).toArray
        val z = x._2.map(_.numHops.toDouble).toArray
        (x._1, y.length, 
          Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), 
          Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z))
      }).collect
      Utils.writeToFile(dstsStats, getFilename(FN_PREFIX, "dstsStats", FN_SUFFIX))

      /************************
       * Individual countries *
       ************************/

      // Calculate bigrams
      val bigrams = GeoASTraces.flatMap(h => Utils.emitDirectedLinks(h.hops, h.segmentLats)).groupBy(_._1).map(l => {
        val y = l._2.map(_._2).toArray
        (l._1, y.length,
          Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y)
          )
      }).collect
      Utils.writeToFile(bigrams, getFilename(FN_PREFIX, "bigrams", FN_SUFFIX))
      
      // Calculate degree distribution
      val degDist = bigrams.map(_._1).groupBy(_.split("\\s+")(0)).map(y => (y._1, y._2.toSet.size.toDouble)).toArray.sortWith(_._2 > _._2)
      Utils.writeToFile(degDist, getFilename(FN_PREFIX, "degDist", FN_SUFFIX))

      /**************************
       * Edge hop contributuons *
       **************************/

      // Considering only those Geo pairs that have more than one ASHops between them
      val edgeStats = pairs.filter(x => Utils.min(x._2.map(_.numHops.toDouble).toArray) > 1).map(x => {
        val lat = x._2.map(_.e2eLatency).toArray
        val fas = x._2.map(y => y.segmentLats(0) / y.e2eLatency * 100.0).toArray
        val f2s = x._2.map(y => y.segmentLats(1) / y.e2eLatency * 100.0).toArray
        val l2s = x._2.map(y => y.segmentLats(y.segmentLats.length - 2) / y.e2eLatency * 100.0).toArray
        val las = x._2.map(y => y.segmentLats(y.segmentLats.length - 1) / y.e2eLatency * 100.0).toArray
        val ina = x._2.map(_.inASLat).toArray
        (x._1, fas.length,
          (Utils.min(lat), Utils.max(lat), Utils.average(lat), Utils.stdev(lat), Utils.coeffOfVar(lat), Utils.percentiles(lat)),
          (Utils.min(fas), Utils.max(fas), Utils.average(fas), Utils.stdev(fas), Utils.coeffOfVar(fas), Utils.percentiles(fas)),
          (Utils.min(f2s), Utils.max(f2s), Utils.average(f2s), Utils.stdev(f2s), Utils.coeffOfVar(f2s), Utils.percentiles(f2s)),
          (Utils.min(l2s), Utils.max(l2s), Utils.average(l2s), Utils.stdev(l2s), Utils.coeffOfVar(l2s), Utils.percentiles(l2s)),
          (Utils.min(las), Utils.max(las), Utils.average(las), Utils.stdev(las), Utils.coeffOfVar(las), Utils.percentiles(las)),
          (Utils.min(ina), Utils.max(ina), Utils.average(ina), Utils.stdev(ina), Utils.coeffOfVar(ina), Utils.percentiles(ina))
          )
      }).collect
      Utils.writeToFile(edgeStats, getFilename(FN_PREFIX, "edgeStats", FN_SUFFIX))
    }
  }
}
