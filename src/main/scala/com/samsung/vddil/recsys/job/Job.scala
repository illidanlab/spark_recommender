/**
 * This file includes the basic definition of a job. 
 */
package com.samsung.vddil.recsys.job

import java.io.File
import java.io.InputStream 
import scala.xml._

import com.samsung.vddil.recsys._

import org.apache.spark.SparkContext
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

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
	
	/*
	 * Generate job XML from current information to be written in file. 
	 */
	def generateXML():Option[scala.xml.Elem]
}


object Job{

	//return input stream from hdfs
	def inputStreamHDFS(hadoopConf:Configuration, filePath:String): Option[InputStream] = {
	   val fileSystem = FileSystem.get(hadoopConf)
	   val path = new Path(filePath)
	   if (fileSystem.exists(path))
		   Some(fileSystem.open(path))
	   else
	       None
	}

	/*
	 * Read jobs from a job file. 
	 */
	def readFromXMLFile(filename:String, sc:SparkContext):List[Job] = {
	  val logger = Logger
	  logger.info("Loading XML file: " + filename)
	  
	  var jobList:List[Job] = List() 
	  var xml:Option[scala.xml.Elem] = None
      val hadoopConf = sc.hadoopConfiguration

      //reading job file from hdfs 
      val jobfileIn:Option[InputStream] = inputStreamHDFS(hadoopConf, filename)
	  
	  // If the job file exists, we parse it and look for all job entries, and 
	  // populate these entries into classes. 
	  if (jobfileIn.isDefined) {
	    logger.info("Job file found in file system: " + filename)
	    xml = Some(XML.load(jobfileIn.get))
	  }else{
		//val resLoc = "/jobs/test_job.xml"
	    val resLoc = "/jobs/"+filename
	    logger.info("Job file not found in file system, try loading resource: [%s]".format(resLoc) )
	    //if file is not found we try to see if we can use one in resource folder.
	    try{
		    val IS = getClass().getResourceAsStream(resLoc);
		    xml = Some(XML.load(IS))
		    
		    logger.info("Job file found in resource!")
	    }catch{
	        case th:Throwable => logger.warn("Job file not found!") 
	    }
	  }
	  
	  
	  if (xml.isDefined){
	    
	    // look for all job entries in the job list. 
	    for (node <- xml.get \ JobTag.JobEntry){
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
    
    def generateXML():Option[Elem] = {
       None
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

/*
 * more compact class to represent rating than Tuple3[Int, Int, Double]
 */
case class Rating(user: Int, item: Int, rating: Double)
