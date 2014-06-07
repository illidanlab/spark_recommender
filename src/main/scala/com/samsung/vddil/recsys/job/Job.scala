/**
 * This file includes a set of job descriptions. 
 */
package com.samsung.vddil.recsys.job

import java.io.File

import scala.xml._

import com.samsung.vddil.recsys._

/*
 * Define a set of types 
 */
object JobType extends Enumeration {
     	type JobType = Value
     	val Recommendation = Value(JobTag.JOB_TYPE_RECOMMENDATION) 
     	val HelloWorld = Value(JobTag.JOB_TYPE_HELLOWOLRD)
     	val Unknown = Value
}
import JobType._

/**
 * The trait of Job class.
 * 
 * @author jiayu.zhou
 *
 */
trait Job {
	/*
	 * The name of the job and will be used as a part of the file name.
	 */
	def jobName:String
	
	/*
	 * The description of the job. 
	 */
	def jobDesc:String
	
	/*
	 * The type of the job. 
	 */
	def jobType:JobType
	
	/*
	 * Run job
	 */
	def run():Unit
}


object Job{
  
	/*
	 * Read jobs from a job file. 
	 */
	def readFromXMLFile(filename:String):List[Job] = {
	  
	  val logger = Logger
	  logger.info("Loading XML file: " + filename)
	  
	  var jobList:List[Job] = List() 
	  
	  val jobfile:File = new File(filename)
	  
	  // If the job file exists, we parse it and look for all job entries, and 
	  // populate these entries into classes. 
	  if (jobfile.exists()) {
	    logger.info("Job file found!")
	    
	    
	    val xml = XML.loadFile(jobfile)
	    // look for all job entries in the job list. 
	    for (node <- xml \ "jobEntry"){
	        val jobTypeStr:String = (node \ JobTag.JOB_TYPE).text
	        val jobNameStr:String = (node \ JobTag.JOB_NAME).text
	        val jobDescStr:String = (node \ JobTag.JOB_DESC).text
	        
	        var jobEntry:Job = null
	           if (jobTypeStr.equals(JobType.Recommendation.toString())){
	             jobEntry = RecJob(jobNameStr, jobDescStr, node)
	           }else if (jobTypeStr.equals(JobType.HelloWorld.toString())){
	             jobEntry = HelloWorldJob(jobNameStr, jobDescStr, node)
	           }else{
	             jobEntry = UnknownJob(jobNameStr, jobDescStr, node)
	           }
	           
	           jobList = jobList ::: List(jobEntry)
	    }
	    
	    
	    logger.info("There are ["+ jobList.length + "] Jobs found. ")
	  }else{
	    logger.warn("Job file not found!")
	  }
	  
	  jobList
	}
	
}

/*
 * Unknown Job
 */
case class UnknownJob (jobName:String, jobDesc:String, jobNode:Node) extends Job{
    val jobType = JobType.Unknown
    
    override def toString():String = {
       "Job:Unknown [" + this.jobName + "]"
    }
    
    def run():Unit = {
       //do nothing.
    }
}


