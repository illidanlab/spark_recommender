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
     	val Recommendation = Value(JobTag.JobType_Recommendation) 
     	val HelloWorld = Value(JobTag.JobType_HelloWorld)
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
	
	/*
	 * Get the current job status. 
	 */
	def getStatus():JobStatus
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
	    for (node <- xml \ JobTag.JobEntry){
	        val jobTypeStr:String = (node \ JobTag.JobType).text 
	        val jobNameStr:String = (node \ JobTag.JobName).text
	        val jobDescStr:String = (node \ JobTag.JobDesc).text
	        
	        var jobEntry:Job = jobTypeStr match{
	          case JobTag.JobType_Recommendation => RecJob(jobNameStr, jobDescStr, node) 
	          case JobTag.JobType_HelloWorld     => HelloWorldJob(jobNameStr, jobDescStr, node)
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
 * Unknown Job. This is a dummy implementation show how things could work. 
 */
case class UnknownJob (jobName:String, jobDesc:String, jobNode:Node) extends Job{
    val jobType = JobType.Unknown
    val jobStatus = new UnknownJobStatus(this)
    
    override def toString():String = {
       "Job:Unknown [" + this.jobName + "]"
    }
    
    def run():Unit = {
       //do nothing.
    }
    
    def getStatus():JobStatus = {
       return this.jobStatus
    }
}

case class UnknownJobStatus(jobInfo:UnknownJob) extends JobStatus{
	def allCompleted():Boolean = {
       true
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("Unknown job [%s] has completed.".format(jobInfo.jobName))
    }
}


