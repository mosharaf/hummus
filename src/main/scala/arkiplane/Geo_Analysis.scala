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


object Geo_Analysis {

  private def getFilename(prefix: String, name: String, suffix: String) = {
    prefix + "-" + name + "." + suffix
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: Geo_Analysis <ip-X-X-X-X> <ARK|IPLANE>")
      System.exit(1)
    }

    val ipx =  args(0)
    val SPARK_PATH = "spark://" + ipx + ":7077"
    val HDFS_PATH = "hdfs://" + ipx + ":9010/"

    val DATASET_NAME = args(1).toUpperCase() match {
      case "ARK" => ARK
      case "IPLANE" => IPLANE
    }

    val cityOrCountry = "City".toUpperCase()
    val FN_SUFFIX = {
      if (cityOrCountry == "CITY")
        "City"
      else
        "Country"
    }
    val MAPPING_SUFFIX = {
      if (cityOrCountry == "CITY")
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
    
    val sc = new SparkContext(SPARK_PATH, "Geo_Analysis_ARKIPLANE", "/root/spark", Seq("/root/bigtrace/target/scala-2.9.3/bigtrace_2.9.3-0.0.jar"))

    // Create IP-to-AS mapping    
    val PATH_TO_MAPPING_FILE = "" + MAPPING_SUFFIX
    val ipMap = LineReaders.ipEdgescapeTreeMapNew(sc, PATH_TO_MAPPING_FILE)
    val ipMap_bc = sc.broadcast(ipMap)
    
    var traceLines = sc.textFile(HDFS_PATH + "/" + DATASET_NAME)

    // Preprocessing for ARK (Keep those without error codes)
    if (DATASET_NAME == ARK) {      
      traceLines = traceLines.filter(_.contains("MOS"))
    }

    // Remove 0.0.0.0
    traceLines = traceLines.filter(x => !x.contains("0.0.0.0") && !x.contains("-"))

    val GeoTracesAll = traceLines.flatMap(LineReaders.newTraceDetailed_GeoLevel(_, DATASET_NAME, ipMap_bc.value)).cache

    for (year <- List("2008", "2009", "2010", "2011", "2012", "2013")) {
      val GeoTraces = {
        if (year == "ALL")
          GeoTracesAll
        else
          GeoTracesAll.filter(x => Utils.monthFromTimestamp(x.timestamp).contains(year))
      }
      val FN_PREFIX = DATASET_NAME + "-" + year

      /*************************
       * Groupby Src-Dst Pairs *
       *************************/

      val pairs = GeoTraces.groupBy(x => (x.from, x.to))//.filter(_._2.size >= 12)
      val pairStats = pairs.map(x => {
          val y = x._2.map(_.e2eLatency).toArray
          val z = x._2.map(_.numHops.toDouble).toArray
          val a = x._2.map(_.hopIndices.last.toDouble).toArray
          val q = x._2.sortBy(_.timestamp).map(w => Utils.arrToString(w.hops.toArray.map(_.toString))).toArray
          (x._1, y.length, 
            Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y),
            Utils.min(z), Utils.max(z), Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z), Utils.percentiles(z),
            Utils.min(a), Utils.max(a), Utils.average(a), Utils.stdev(a), Utils.coeffOfVar(a), Utils.percentiles(a),
            Utils.pathPrevalence(q)
            )
        })
      pairStats.saveAsTextFile(HDFS_PATH + "/" + getFilename(FN_PREFIX, "pairStats", FN_SUFFIX))

      /*******************
       * Groupby Sources *
       *******************/
      val srcs = GeoTraces.groupBy(_.from)
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
      val dsts = GeoTraces.groupBy(_.to).cache
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
      val bigrams = GeoTraces.flatMap(h => Utils.emitDirectedLinks(h.hops, h.segmentLats)).groupBy(_._1).map(l => {
        val y = l._2.map(_._2).toArray
        (l._1, y.length,
          Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y)
          )
      }).collect
      Utils.writeToFile(bigrams, getFilename(FN_PREFIX, "bigrams", FN_SUFFIX))
    }
  }
}
