package com.samsung.vddil.recsys.job

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.Logger

/*
 * This is an example showing how to implement a job. 
 */
case class HelloWorldJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
    val jobType = JobType.HelloWorld
    val jobStatus:HelloWorldJobStatus = new HelloWorldJobStatus(this)  
    
    override def toString():String = {
       "Job:HelloWorld [" + this.jobName + "]"
    }
    
    def run():Unit = {
       // do some spark test. 
        println("Test Spark Libraries: ")
      
        val inputFileStr = System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad.txt";
        val outputFileStr= System.getProperty("user.home") + "/workspace/data/wordcount/adv_alad_cnt_eclipse_maven_proj.txt";
		
        try{
        	val sc = new SparkContext("local[4]", "TestWordCount")    
	   
			val lines = sc.textFile(inputFileStr)
			
			//println(System.getProperty("user.home"))
			
		    
			//val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_)
		    
		    //val counts = lines.flatMap(_.split(" ")).map(e => (e, 1)).reduceByKey(_+_).map{case (e1, e2) => ""+ e1 + "\t" + e2 }
		    //counts.saveAsTextFile(outputFileStr)
        }catch{
          case e: Exception => {
            this.jobStatus.status(HelloWorldJobStatus.STAGE_TEST_SPARK) = false
          }
        }
        this.jobStatus.status(HelloWorldJobStatus.STAGE_TEST_SPARK) = true
    }
    
    def getStatus():HelloWorldJobStatus = {
       return this.jobStatus
    }
    
    def generateXML():Elem = {
       null
    }
}

/*
 * An example of implementing the JobStatus. 
 */
case class HelloWorldJobStatus(jobInfo:HelloWorldJob) extends JobStatus{
	
	  
	var status:HashMap[Any, Boolean] = new HashMap()
	this.status(HelloWorldJobStatus.STAGE_TEST_SPARK) = false
	
	/*
	 * Since we only have one thing to do, allCompleted just return 
	 * all the 
	 */
	def allCompleted():Boolean = {
       this.status(HelloWorldJobStatus.STAGE_TEST_SPARK)
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("The status of current job [completed? %s]".format(allCompleted().toString))
    }
}

object HelloWorldJobStatus{
    val STAGE_TEST_SPARK = "test_spark"
}


