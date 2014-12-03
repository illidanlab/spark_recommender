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

import org.apache.spark.HashPartitioner

/**
* Add some comments for test
*/
object RecOutput {
  // program id, channel id, start time. 
  type RoviType = (String, (String, Long))
  // duid, program id, rating.
  type RecType = (Int, (String, Double))
  // duid, programs. 
  type OutputType = (Int, Seq[(String, String, Long, Double)]) 
  
  type JoinedType = (String, ((Int, Double), (String, Long)))

  var sc: SparkContext = null

  def main(args: Array[String]) {
    val date = "20140804"
    val roviPath = s"s3://vddil.data.standard/apps/vddil/rovi_hq/${date}/schedule.txt.gz"
    val currentHour = "201408040100"
    val inputPath = "s3://vddil.recsys.east/input_list.txt"
    //val outputPath = "s3://vddil.recsys.east/output/"
    val currentTime = System.currentTimeMillis()
    val outputPath = s"hdfs:///output-${currentTime}/"
    val programMapPath = outputPath + "program-map.txt"
    val appName = "OutputRec"     
    val conf = new SparkConf().setAppName(appName)
        .set("spark.executor.extraJavaOptions ", "-XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "com.samsung.vddil.recsys.SerializationRegistrator")
        .set("spark.yarn.executor.memoryOverhead", "768")
        .set("spark.rdd.compress", "true")
        .set("spark.storage.memoryFraction", "0.2")
        .set("spark.core.connection.ack.wait.timeout", "600")
        .set("spark.akka.frameSize", "50")
        .set("spark.executor.memory", "30g")
        .set("spark.shuffle.spill", "true")

    sc = new SparkContext(conf)
    val paraSc: SparkContext = null
    val K = 3
    val programScorePaths = sc.textFile(inputPath).collect
    doRecommendation(paraSc, date, roviPath, inputPath, programScorePaths, K)
  }

  // load rdd from a list of block files.
  def loadProgramScores(paraSc: SparkContext, programScorePaths: List[String]) = {
    //println(programScorePaths.mkString("\n"))
    var programScores: RDD[RecType] = paraSc.objectFile[RecType](programScorePaths(0))
    val dirNum = programScorePaths.size
    // last index of the input score files.
    for (idx <- 1 until dirNum) {
      val programScorePath = programScorePaths(idx)
      programScores = programScores.union(paraSc.objectFile[RecType](programScorePath))
    }
    programScores
  }
  // the main function to do recommendation. called from other classes.
  def doRecommendation(paraSc: SparkContext, date: String, roviPath: String, 
                       programScorePaths: List[String], outputPath: String, K: Int) {
    //sc = paraSc
    val programScores = loadProgramScores(paraSc, programScorePaths)
    val startHourString = date + "0000"
    val startHour = timestampToUTCUnixTime(startHourString)
    for (i <- 0 until 24) {
      val currentHour = startHour + i * 3600
      val finalPath = f"${outputPath}/${i}%02d"  
      recommend(roviPath, currentHour, programScores, finalPath, K)
    }
  }
  

  // load the map from program id to an integer from hdfs files.
  def loadProgramMap(path: String): RDD[(String, Int)] = {
    val programMap = sc.textFile(path).map(_.split("\t"))
                                      .map(data => (data(0), data(1).toInt))
    return programMap
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

  // load rovi data as a hash map. 
  def loadRoviDataAsMap(roviPath: String, currentHour: Long): scala.collection.Map[String, (String, Long)] = {
    val filteredRoviData = loadRoviData(roviPath, currentHour)
    val roviMap = filteredRoviData.collectAsMap()
    return roviMap
  }

  def saveRddAsBlocks(
    dataset: RDD[JoinedType], 
    blockNum: Int, 
    blockFilePath: String): Array[String] =  {
    val partitionedRDD = dataset.repartition(blockNum)
    val blockSize      = partitionedRDD.partitions.size
    val blockFiles     = new Array[String](blockSize) 
        
        
    for ((dataBlock, blockId) <- dataset.partitions.zipWithIndex){
      val idx = dataBlock.index
      val blockRDD = partitionedRDD.mapPartitionsWithIndex(
                    (ind, x) => if (ind == idx) x else Iterator(), true)
            val blockFile = s"${blockFilePath}/block${idx}" 
            //be sure to check if file exists or not. 
            blockFiles(blockId) = blockFile 
            blockRDD.saveAsObjectFile(blockFile)
        }
        
        blockFiles
  }

  def loadBlocksAsRdd(blockFiles: Array[String]): RDD[JoinedType]  = {
    val fileNum = blockFiles.size
    assert(fileNum != 0)
    var rdd = sc.objectFile[JoinedType](blockFiles(0))
    for (i <- 1 until fileNum) {
      val block = sc.objectFile[JoinedType](blockFiles(i))
      rdd = rdd.union(block)
    }
    return rdd
  }
  
  // keep only programs that will be shown a certain portion within this hour. 
  def getRec(programScores: RDD[RecType], 
             roviData: RDD[RoviType], K: Int): RDD[OutputType] = {
    val roviMap = roviData.collectAsMap()
    val partitionedProgramScores = programScores.map{case(duid, (program, score))
                                                      => (program, (duid, score))}
                                                .filter{case(program, (duid, score))
                                                          => roviMap.contains(program)}  
                                                //.repartition(400)
                                                //.partitionBy(new HashPartitioner(400)).persist()
    val joinedRec = partitionedProgramScores.map{case(program, (duid, score))
                                                   => (program, ((duid, score), roviMap(program)))}          
    val blockFilePath = "hdfs:///output.blocks/" 
    val wholeFilePath = "hdfs:///output.whole/"
    val blockNum = 400
    //joinedRec.saveAsObjectFile(wholeFilePath)
    //val loadedJoinedRec = sc.objectFile[JoinedType](wholeFilePath)
    //val blockFiles = saveRddAsBlocks(joinedRec, blockNum, blockFilePath)
    //val loadedJoinedRec = loadBlocksAsRdd(blockFiles)
    val groupedRec = joinedRec.map{case(program, ((duid, score), (channel, startTime))) 
                                     => (duid, Set((program, channel, startTime, score)))}
                              .reduceByKey((a, b) => a.union(b)) 
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
