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


object AS_Analysis {

  private def getFilename(prefix: String, name: String, suffix: String) = {
    prefix + "-" + name + "." + suffix
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: AS_Analysis <ip-X-X-X-X> <ARK|IPLANE> [AS|LOC]")
      System.exit(1)
    }

    val ipx =  args(0)
    val SPARK_PATH = "spark://" + ipx + ":7077"
    val HDFS_PATH = "hdfs://" + ipx + ":9010/"

    val DATASET_NAME = args(1).toUpperCase() match {
      case "ARK" => ARK
      case "IPLANE" => IPLANE
    }

    var locOrAS = "AS".toUpperCase()
    if (args.length > 2)
      locOrAS = args(2).toUpperCase()

    val FN_SUFFIX = {
      if (locOrAS == "CITY")
        "City"
      else
        "AS"
    }
    
    // Setup system properties
    System.setProperty("spark.storage.blockManagerTimeoutIntervalMs", "300000")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.worker.timeout", "300")
    System.setProperty("spark.executor.memory", "56G")
    System.setProperty("spark.akka.frameSize", "1024")
    
    val sc = new SparkContext(SPARK_PATH, "AS_Analysis_ARKIPLANE", "/root/spark", Seq("/root/bigtrace/target/scala-2.9.3/bigtrace_2.9.3-0.0.jar"))

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

    val asTracesAll = {
      if (locOrAS == "CITY")
        traceLines.flatMap(LineReaders.newTraceDetailed_GeoLevel(_, DATASET_NAME, ipMap_bc.value, true)).cache
      else
        traceLines.flatMap(LineReaders.newTraceDetailed_GeoASLevel(_, DATASET_NAME, ipMap_bc.value, false)).cache
    }

    for (year <- List("2008", "2009", "2010", "2011", "2012", "2013")) {
      val asTraces = {
        if (year == "ALL")
          asTracesAll
        else
          asTracesAll.filter(x => Utils.monthFromTimestamp(x.timestamp).contains(year))
      }
      val FN_PREFIX = DATASET_NAME + "-" + year

      /*********************
       * Across ALL traces *
       *********************/

      val numTraces = asTraces.count
      Utils.writeToFile(Array(numTraces), getFilename(FN_PREFIX, "numTraces", FN_SUFFIX))
      System.out.println("numTraces = " + numTraces)

      // Average number of AS hops
      val avgNumASHops = asTraces.map(_.numHops.toLong).reduce(_ + _) / numTraces
      Utils.writeToFile(Array(avgNumASHops), getFilename(FN_PREFIX, "avgNumASHops", FN_SUFFIX))
      System.out.println("avgNumASHops = " + avgNumASHops)

      // AS hop count distribution
      val numASHopDist = asTraces.groupBy(_.numHops).map(y => (y._1, y._2.size)).collect
      Utils.writeToFile(numASHopDist, getFilename(FN_PREFIX, "numASHopDist", FN_SUFFIX))
      
      // Average number of hops
      val avgNumHops = asTraces.map(_.hopIndices.last.toLong).reduce(_ + _) / numTraces
      Utils.writeToFile(Array(avgNumHops), getFilename(FN_PREFIX, "avgNumHops", FN_SUFFIX))
      System.out.println("avgNumHops = " + avgNumHops)

      // Hop count distribution
      val numHopDist = asTraces.groupBy(_.hopIndices.last).map(y => (y._1, y._2.size)).collect
      Utils.writeToFile(numHopDist, getFilename(FN_PREFIX, "numHopDist", FN_SUFFIX))

      // Average latency across all traces
      val avgE2ELatency = asTraces.map(_.e2eLatency).reduce(_ + _) / numTraces
      Utils.writeToFile(Array(avgE2ELatency), getFilename(FN_PREFIX, "avgE2ELatency", FN_SUFFIX))
      System.out.println("avgE2ELatency = " + avgE2ELatency)

      // Latency distribution
      val e2eLatDist = asTraces.groupBy(_.e2eLatency.toInt).map(y => (y._1, y._2.size)).collect
      Utils.writeToFile(e2eLatDist, getFilename(FN_PREFIX, "e2eLatDist", FN_SUFFIX))

      /*************************
       * Groupby Src-Dst Pairs *
       *************************/

      // Cutting at 100
      val pairs = asTraces.groupBy(x => (x.from, x.to)).filter(_._2.size >= 100)
      val pairStats = pairs.map(x => {
          val y = x._2.map(_.e2eLatency).toArray
          val z = x._2.map(_.numHops.toDouble).toArray
          val a = x._2.map(_.hopIndices.last.toDouble).toArray
          val q = x._2.sortBy(_.timestamp).map(w => Utils.arrToString(w.hops.toArray.map(_.toString), false)).toArray
          (x._1, y.length, 
            Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y),
            Utils.min(z), Utils.max(z), Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z), Utils.percentiles(z),
            Utils.min(a), Utils.max(a), Utils.average(a), Utils.stdev(a), Utils.coeffOfVar(a), Utils.percentiles(a),
            Utils.pathPrevalence(q)
            )
        }).collect
      Utils.writeToFile(pairStats, getFilename(FN_PREFIX, "pairStats", FN_SUFFIX))

      /*******************
       * Groupby Sources *
       *******************/
      val srcs = asTraces.groupBy(_.from)
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
      val dsts = asTraces.groupBy(_.to).cache
      val dstsStats = dsts.map(x => {
        val y = x._2.map(_.e2eLatency).toArray
        val z = x._2.map(_.numHops.toDouble).toArray
        (x._1, y.length, 
          Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), 
          Utils.average(z), Utils.stdev(z), Utils.coeffOfVar(z))
      }).collect
      Utils.writeToFile(dstsStats, getFilename(FN_PREFIX, "dstsStats", FN_SUFFIX))

      // Number of ASes before the last AS (Multihoming?)
      val multihoming = dsts.map(x => {
        val z = x._2.flatMap(y => {
          if (y.numHops >= 2) Some(y.hops(y.numHops - 2))
          else None
        }).toSet
        (x._1, z.size, z)
      }).collect
      Utils.writeToFile(multihoming, getFilename(FN_PREFIX, "multihoming", FN_SUFFIX))

      /*******************
       * Individual ASes *
       *******************/

      // Calculate bigrams
      val bigrams = asTraces.flatMap(h => Utils.emitDirectedLinks(h.hops, h.segmentLats)).groupBy(_._1).map(l => {
        val y = l._2.map(_._2).toArray
        (l._1, y.length,
          Utils.min(y), Utils.max(y), Utils.average(y), Utils.stdev(y), Utils.coeffOfVar(y), Utils.percentiles(y)
          )
      }).collect
      Utils.writeToFile(bigrams, getFilename(FN_PREFIX, "bigrams", FN_SUFFIX))

      // Calculate degree distribution
      val degDist = bigrams.map(_._1).groupBy(_.split("\\s+")(0)).map(y => (y._1, y._2.toSet.size.toDouble)).toArray.sortWith(_._2 > _._2)
      Utils.writeToFile(degDist, getFilename(FN_PREFIX, "degDist", FN_SUFFIX))
    }
  }
}
