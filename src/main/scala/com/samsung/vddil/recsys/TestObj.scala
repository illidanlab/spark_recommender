package com.samsung.vddil.recsys

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestObj {

  def main(args: Array[String]): Unit = {
    
		println("Hell, yeah...")
    
		//println(System.getProperty("user.home"))
		
		val inputFileStr = System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad.txt";
		val outputFileStr= System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad_cnt_eclipse_maven_proj.txt";
		
	    val sc = new SparkContext("local[4]", "TestWordCount")    
	   
		val lines = sc.textFile(inputFileStr)
	    
	    val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_).map{case (e1, e2) => ""+ e1 + "\t" + e2 }
//	    
	    counts.saveAsTextFile(outputFileStr)
//    
    
    
  }

}