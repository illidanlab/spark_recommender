package com.samsung.vddil.recsys.job

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.w3c.dom.{Document, Element, Node, NodeList}

case class HelloWorldJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
    val jobType = JobType.HelloWorld
    
    override def toString():String = {
       "Job:HelloWorld [" + this.jobName + "]"
    }
    
    def run():Unit = {
       // do some spark test. 
        println("Test Spark Libraries: ")
      
        val inputFileStr = System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad.txt";
        val outputFileStr= System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad_cnt_eclipse_maven_proj.txt";
		
	    val sc = new SparkContext("local[4]", "TestWordCount")    
	   
		val lines = sc.textFile(inputFileStr)
		
		//println(System.getProperty("user.home"))
		
	    
		//val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_)
	    
	    //val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_).map{case (e1, e2) => ""+ e1 + "\t" + e2 }
	    //counts.saveAsTextFile(outputFileStr)

    }
}