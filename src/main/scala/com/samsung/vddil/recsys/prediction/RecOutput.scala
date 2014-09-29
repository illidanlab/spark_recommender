package com.samsung.vddil.recsys.prediction

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}
import java.util.Calendar
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util._
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.time.DateUtils

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.mutable.Map
import scala.sys.process._

/**
* Add some comments for test
*/
object RecOutput {
  // program id, start time, finish time.
  type RoviType = (String, (String, Long))
  // duid, program id, rating.
  type RecType = (Int, String, Double)
  // duid, programs. 
  type OutputType = (Int, Seq[(String, String, Long, Double)]) 

  var sc: SparkContext = null

  def main(args: Array[String]) {
    val date = "20140901"
    val roviPath = s"hdfs://gnosis-01-01-01.crl.samsung.com/apps/vddil/rovi_hq/${date}/schedule.txt.gz"
    val currentHour = "201409011000"
    val hadoopPath = "hdfs://gnosis-01-01-01.crl.samsung.com/apps/data/vddil/rec-output/"
    val programMapPath = hadoopPath + "program-map.txt"
    val programScoresPath = hadoopPath + "program-scores.txt"
    val outputPath = hadoopPath + "output/" + date
    val appName = "OutputRec"     
    val conf = new SparkConf().setAppName(appName)
    sc = new SparkContext(conf)
    val paraSc: SparkContext = null

    //val programMap = loadProgramMap(programMapPath)  
    //programMap.saveAsTextFile(hadoopPath+"program-map")
    val programScores = loadProgramScores(programScoresPath)  
    //programScores.saveAsTextFile(hadoopPath+"program-scores")
    val K = 1
    doRecommendation(paraSc, date, roviPath, programScores, outputPath, K)
  }

  // the main function to do recommendation. called from other classes.
  def doRecommendation(paraSc: SparkContext, date: String, roviPath: String, 
                       programScores: RDD[RecType], outputPath: String, K: Int) {
    sc = paraSc
    val partitionedProgramScores = programScores.partitionByKey(new HashPartitioner(100))
    val startHourString = date + "0000"
    val startHour = timestampToUTCUnixTime(startHourString)
    for (i <- 0 until 24) {
      val currentHour = startHour + i * 3600
      val finalPath = f"${outputPath}/${i}%02d"  
      recommend(roviPath, currentHour, partitionedProgramScores, finalPath, K)
    }
  }
  

  // load the map from program id to an integer from hdfs files.
  def loadProgramMap(path: String): RDD[(String, Int)] = {
    val programMap = sc.textFile(path).map(_.split("\t"))
                                      .map(data => (data(0), data(1).toInt))
    return programMap
  }

  // load the scores that a duid rates a program from hdfs files.
  def loadProgramScores(path: String): RDD[(Int, String, Double)] = {
    val programScores = sc.textFile(path).map(_.split("\t"))
                          .map(data => (data(0).toInt, data(1), data(2).toDouble))
    return programScores
  }

  def recommend(roviPath: String, currentHour: Long, 
                programScores: RDD[RecType], outputPath: String, K: Int) {
    val roviData = loadRoviData(roviPath, currentHour)
    val rec = getRec(programScores, roviData, K) 
    val recNew = rec.map{case(duid, programList) 
                           => (duid, programList.map{case(program, channel, startTime, score) 
                                                       => (program, channel, unixToUTCTimestamp(startTime), score)})}
    val outputRec = rec.map{case(duid, programList) => Array(duid, programList.map(_._1).mkString("\t")).mkString("\t")}
    outputRec.saveAsTextFile(outputPath)
  }

  def unixToUTCTimestamp(time:Long): String = {
    val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    formater.setTimeZone(TimeZone.getTimeZone("UTC"))
    val timestamp = formater.format(new Date(time*1000L))
    return timestamp
  }

  // load the program information, including start time and finish time.
  def loadRoviData(roviPath: String, currentHour: Long): RDD[RoviType] = {
    // program, start time in seconds, finish time in sedonds.
    val roviData = sc.textFile(roviPath).map(_.split("""\|"""))
                                        .map(data => (data(4), (data(1),
                                                     timestampToUTCUnixTime(data(2)))))
    val filteredRoviData = roviData.filter{case(program, (channel, startTime)) 
                                             => isValidProgram(currentHour, startTime)}
    return filteredRoviData 
  }
  
  // keeo only programs that will be shown a certain portion within this hour. 
  def getRec(programScores: RDD[RecType], 
             roviData: RDD[RoviType], K: Int): RDD[OutputType] = {
    val joinedRec = programScores.map{case(duid, program, score) 
                                        => (program, (duid, score))}.join(roviData) //probably repartition. 
    val groupedRec = joinedRec.map{case(program, ((duid, score), (channel, startTime))) 
                                     => (duid, (program, channel, startTime, score))}
                              .groupByKey() 
    val topPrograms = groupedRec.map{case(duid, programList) 
                                       => (duid, selectTopPrograms(programList.toSeq, K))}
    return topPrograms
  } 
 
  def selectTopPrograms(programList: Seq[(String, String, Long, Double)], K: Int)
                                   : Seq[(String, String, Long, Double)] = {
    val groupedPrograms = programList.groupBy(_._1)
                                     .map{case(program, result) 
                                            => result.sortBy(_._3).head}
                                     .toSeq.sortWith((u ,v) => u._4 > v._4).take(K)
    return groupedPrograms
  }

  // output recommendation result.
  def outputRec(rec: RDD[OutputType], path: String) {
    rec.saveAsTextFile(path)
  }

  // whether the program will be shown within this hour.
  def isValidProgram(currentHour: Long, startTime: Long): Boolean = {
    val diff = startTime - currentHour
    return diff >= 0 && diff < 3600
  }

  def timestampToUTCUnixTime(timestamp:String): Long = {
    val formater = new SimpleDateFormat("yyyyMMddHHmm")
    formater.setTimeZone(TimeZone.getTimeZone("UTC"))
    val unixSeconds = formater.parse(timestamp).getTime()/1000
    return unixSeconds
  }
}
