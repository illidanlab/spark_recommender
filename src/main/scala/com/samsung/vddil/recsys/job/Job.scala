/**
 * This file includes the basic definition of a job. 
 */
package com.samsung.vddil.recsys.job

import java.io.File

import scala.xml._

import com.samsung.vddil.recsys._

/*
 * Define a set of job types 
 */
object JobType extends Enumeration {
     	type JobType = Value
     	val Recommendation = Value(JobTag.JOB_TYPE_RECOMMENDATION) 
     	val HelloWorld = Value(JobTag.JOB_TYPE_HELLOWOLRD)
     	val Unknown = Value
}

import JobType._ 

/** 
 * The trait of Job class. The Job serves as a data structure of jobs 
 * to be executed in the recommendation pipeline and define how the job
 * should be carried out. 
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
	 * This is the main entrance of the run method. 
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
	        
	        var jobEntry:Job = jobTypeStr match{
	          case JobTag.JOB_TYPE_RECOMMENDATION => RecJob(jobNameStr, jobDescStr, node) 
	          case JobTag.JOB_TYPE_HELLOWOLRD     => HelloWorldJob(jobNameStr, jobDescStr, node)
	          case _ => UnknownJob(jobNameStr, jobDescStr, node)
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


