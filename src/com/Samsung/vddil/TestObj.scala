package com.Samsung.vddil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestObj {

  def main(args: Array[String]): Unit = {
    println("Hell, yeah...")
    
    
    val sc = new SparkContext("local[4]", "TestWordCount")
    
    
    val lines = sc.textFile("/Users/jiayu.zhou/workspace/data/wordcount/adv_alad.txt")
    
    val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_).map{case (e1, e2) => ""+ e1 + "\t" + e2 }
    
    counts.saveAsTextFile("/Users/jiayu.zhou/workspace/data/wordcount/adv_alad_cnt_eclipse.txt")
  }

}