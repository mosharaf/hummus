/*
 * Copyright 2014-present Regents of the University of California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable._
import scala.math._

case class Percentiles(
  val p25: Double,
  val p50: Double,
  val p75: Double,
  val p90: Double,
  val p95: Double,
  val p99: Double
)

object Utils {

  def arrToString(arr: Array[String], keepDistinct: Boolean=false) = {
    var x = {
      if (keepDistinct)
        arr.toSet.toList.sorted.toArray
      else
        arr
    }
    val ret = x.foldLeft("") { (x, y) => x + "-" + y}
    ret.substring(1, ret.length)
  }

  def ipToLong(ip: String): Long = {
    var ret = 0L
    val parts = ip.split("\\.")
    for (i <- 0 to 3) {
      ret += parts(i).toLong << ((3 - i) * 8)
    }
    ret
  }

  def netmaskToLong(netmask: String): Long = {
    val p = netmask.split("/")
    val ipLong = ipToLong(p(0))
    val shiftLeft = 32 - p(1).toInt
    ipLong & (0xFFFFFFFFFFFFFFFFL << shiftLeft)
  }
  
  def slash24(ip: String): String = {
    ip.take(ip.lastIndexOf("."))
  }
  
  def slash24(ip: Long): Long = {
    ip & 0xFFFFFF00
  }

  def dateFromTimestamp(timestamp: Long): String = { 
    new SimpleDateFormat("yyyyMMdd").format(timestamp)
  }
  
  def monthFromTimestamp(timestamp: Long): String = { 
    new SimpleDateFormat("yyyyMM").format(timestamp)
  }

  def timestampFromDate(yyyyMMdd: String): Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c = Calendar.getInstance()
    c.setTime(sdf.parse(yyyyMMdd))
    c.getTimeInMillis
  }

  def timestampFromMonth(yyyyMM: String): Long = {
    val sdf = new SimpleDateFormat("yyyyMM")
    val c = Calendar.getInstance()
    c.setTime(sdf.parse(yyyyMM))
    c.getTimeInMillis
  }

  /**
   * Returns: a list of (AS_i, AS_ij)
   */
  def emitDirectedLinks(hops: List[Any], segmentLats: List[Double]): List[(String, Double)] = {
    val links = new ArrayBuffer[(String, Double)]
    for (i <- 1 until hops.length) {
      val bg = "" + hops(i-1) + " " + hops(i)
      val latOnLink = segmentLats(i*2-1)
      links += ((bg, latOnLink))
    }
    links.toList
  }

  def writeToFile(x: Array[_], fileName: String) {
    val pw = new java.io.PrintWriter(fileName)
    x.foreach(pw.println)
    pw.close
  }
  
  def average(x: Array[Double]): Double = {
    x.sum / x.length.toDouble
  }

  def variance(x: Array[Double]): Double = {
    def squaredDifference(value1: Double, value2: Double) = pow(value1 - value2, 2.0)
    val a = average(x)
    x.isEmpty match {
      case false =>
        val squared = x.foldLeft(0.0)(_ + squaredDifference(_, a))
        squared / x.length.toDouble
      case true => 0.0
    }
  }

  def min(x: Array[Double]) = {
    x.reduce(_ min _)
  }
  
  def max(x: Array[Double]) = {
    x.reduce(_ max _)
  }  

  def stdev(x: Array[Double]) = {
    sqrt(variance(x))
  }

  def coeffOfVar(x: Array[Double]): Double = {
    val a = average(x)
    if (a <= 0.0) 0.0 else stdev(x) / a
  }

  def covariance(x: Array[Double], y: Array[Double]): Double = {
    assert(x.size == y.size)
    val xbar = average(x)
    val ybar = average(y)
    val sum_xy = x.zip(y).foldLeft(0.0) {(x, z) => x + (z._1 - xbar) * (z._2 - ybar)}
    sum_xy / x.size.toDouble
  }

  /**
   * Returns: Pearson's correlation coefficent between x and y.
   * x and y should have the same length.
   */
  def pearson(x: Array[Double], y: Array[Double]) = {
    covariance(x, y) / (stdev(x) * stdev(y))
  }

  /**
   * Returns: (efficiency, 0.0, 0)
   *   1 if all paths use the same hops
   *  <1 if paths use different hops
   */
  def hopEfficiency(x: Array[Array[String]]): (Double, Double, Int) = {
    val freqMap = x.flatten.foldLeft(Map[String, Int]() withDefaultValue 0) { (m, x) => m + (x -> (1 + m(x))) }
    val sumFreqs = freqMap.foldLeft(0.0) { (x, y) => x + y._2 }
    val e = freqMap.foldLeft(0.0) { (x, y) => x + (-1.0 * y._2 / sumFreqs) *  log(1.0 * y._2 / sumFreqs) }
    // Divide by alphabet size to calculate efficiency
    (e / log(freqMap.size), 0.0, 0) 
  }

  /**
   * Returns: (efficiency, entropy, numPaths)
   *   0 if all paths are the same
   *  >0 if paths are different
   */
  def pathEfficiency(x: Array[String], justNumPaths: Boolean = false): (Double, Double, Int) = {
    val freqMap = x.foldLeft(Map[String, Int]() withDefaultValue 0) { (m, x) => m + (x -> (1 + m(x))) }
    if (justNumPaths)
      return (0.0, 0.0, freqMap.size)
    val sumFreqs = freqMap.foldLeft(0.0) { (x, y) => x + y._2 }
    val e = freqMap.foldLeft(0.0) { (x, y) => x + (-1.0 * y._2 / sumFreqs) * log(1.0 * y._2 / sumFreqs) }
    (e / log(freqMap.size), e, freqMap.size)
  }

  /**
   * x must come from traces that have already been sorted by timestamp  
   * to have a correct numChanges value.
   * 
   * Returns: (prevalence, numChanges, numPaths, theDominantPath)
   * Prevalence is the probability of observing the dominant path
   * http://conferences.sigcomm.org/sigcomm/1996/papers/paxson.pdf ($7.4)
   */
  def pathPrevalence(x: Array[String]): (Double, Int, Int, String) = {
    var numChanges = 0
    for (i <- 1 until x.size) {
      if (x(i) != x(i-1)) {
        numChanges += 1
      }
    }

    val freqMap = x.foldLeft(Map[String, Int]() withDefaultValue 0) { (m, x) => m + (x -> (1 + m(x))) }
    val sumFreqs = freqMap.foldLeft(0.0) { (x, y) => x + y._2 }
    val maxFreqs = freqMap.foldLeft(0.0) { (x, y) => math.max(x, y._2) }
    (maxFreqs / sumFreqs, numChanges, freqMap.size, freqMap.filter(_._2 == maxFreqs).keys.first)
  }

  /**
   * Returns: 101-point CDF.
   */ 
  def cdfFromSorted(xx: Array[Double]): Array[(Double, Double)] = {
    val x = xx.sorted
    val numElem = x.size
    val delta = if (numElem >= 100) 0.01 else (1.0 / numElem)
    var i = 1
    var p = delta
    val ret = new ArrayBuffer[(Double, Double)]()
    ret += ((0.0, 0.0))
    for (elem <- x) {
      if (i.toDouble / numElem >= p || math.abs(i.toDouble / numElem - p) < 1e-6) {
        ret += ((math.round(p * 100.0) / 100.0, elem))
        p += delta
      }
      i += 1
    }
    ret.toArray
  }
  
  /**
   *  Returns: a Percentiles object.
   */
  def percentiles(x: Array[Double]): Percentiles = {
    val cdfC = cdfFromSorted(x)
    assert(cdfC.length <= 101)
    
    if (cdfC.length == 101) {
      val cdf = cdfC.map(_._2)
      Percentiles(cdf(25), cdf(50), cdf(75), cdf(90), cdf(95), cdf(99))
    } else {
      var pVals = new ArrayBuffer[Double]()
      for (p <- Array(0.25, 0.5, 0.75, 0.9, 0.95, 0.99)) {
        var found = false
        for (i <- 0 until cdfC.length) {
          if (!found && cdfC(i)._1 >= p) {
            val ydiff = cdfC(i)._1 - cdfC(i - 1)._1
            val xdiff = cdfC(i)._2 - cdfC(i - 1)._2
            val slope = if (ydiff > 0.0) xdiff / ydiff else 0.0
            pVals += cdfC(i - 1)._2 + slope * (p - cdfC(i - 1)._1)
            found = true
          }
        }
      }
      Percentiles(pVals(0), pVals(1), pVals(2), pVals(3), pVals(4), pVals(5))
    }
  }

  /**
   * Returns: true if a path has a cycle; otherwise, false.
   * A path is a sequence of hops separated by dash.
   */
  def pathHasCycle(path: String): Boolean = {
    val splits = path.split("-")
    splits.length != splits.toSet.size
  }
}
