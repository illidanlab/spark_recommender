package com.samsung.vddil.recsys.job

import java.io.File
import java.io.InputStream
import scala.xml._
import com.samsung.vddil.recsys._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import com.samsung.vddil.recsys.utils.Logger



/**
 * Define a set of job types 
 */
object JobType extends Enumeration {
     	type JobType = Value
     	val Recommendation = Value(JobTag.JobType_Recommendation) 
     	val HelloWorld = Value(JobTag.JobType_HelloWorld)
     	val TemporalAgg = Value(JobTag.JobType_TemporalAgg)
      val Unknown = Value
}
import JobType._

/** 
 * The trait of Job class. The Job serves as a data structure of jobs 
 * to be executed in the recommendation pipeline and define how the job
 * should be carried out. 
 *
 */
trait Job {
	/** The name of the job and will be used as a part of the file name. */
	def jobName:String
	
	/** The description of the job.  */
	def jobDesc:String
	
	/** The type of the job.  */
	def jobType:JobType
	
	/** Carries out the job execution. */
	def run():Unit
	
	/** Gets the current job status. */
	def getStatus():JobStatus
	
	/** Generates job XML from current information to be written in file. */
	def generateXML():Option[scala.xml.Elem]
}

/**
 * Static functions used in job parsing and file handling. 
 */
object Job{

	/**
	 * Returns the input stream from Hadoop file system, which is used for 
	 * reading files from Hadoop. 
	 * 
	 * @param hadoopConf the configuration of Hadoop 
	 * @param filePath the path of the file
	 * 
	 * @return input stream from HDFS
	 */
	def inputStreamHDFS(hadoopConf:Configuration, filePath:String): Option[InputStream] = {
	   val fileSystem = FileSystem.get(hadoopConf)
	   val path = new Path(filePath)
	   if (fileSystem.exists(path))
		   Some(fileSystem.open(path))
	   else
	       None
	}

	/**
	 * Read jobs from a job file. 
	 * 
	 * The method firstly tries to read the file from HDFS. If failed, then it will 
	 * will try to find the file in resource folder. 
	 * 
	 * @param filename the name of the XML file
	 * @param sc SparkContext
	 * 
	 * @return a list of jobs to be executed
	 */
	def readFromXMLFile(filename:String, sc:SparkContext):List[Job] = {
	  val logger = Logger
	  logger.info("Loading XML file: " + filename)
	  
	  var jobList:List[Job] = List() 
	  var xml:Option[scala.xml.Elem] = None
      val hadoopConf = sc.hadoopConfiguration

      //reading job file from HDFS 
      val jobfileIn:Option[InputStream] = inputStreamHDFS(hadoopConf, filename)
	  
	  // If the job file exists, we parse it and look for all job entries, and 
	  // populate these entries into classes. 
	  if (jobfileIn.isDefined) {
	    logger.info("Job file found in file system: " + filename)
	    xml = Some(XML.load(jobfileIn.get))
	  }else{
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
	            //register Job types. 
	            case JobTag.JobType_Recommendation => RecJob(jobNameStr, jobDescStr, node) 
	            case JobTag.JobType_HelloWorld     => HelloWorldJob(jobNameStr, jobDescStr, node)
              case JobTag.JobType_TemporalAgg    => TemporalAggJob(jobNameStr, jobDescStr, node)  
              case _ => UnknownJob(jobNameStr, jobDescStr, node)
	        } 
	           
	        jobList = jobList ::: List(jobEntry)
	    }

	    logger.info("There are ["+ jobList.length + "] Jobs found. ")	    
	  }
	  
	  jobList
	}
}

/**
 * Creates an unknown Job. 
 * 
 * The unknown Job is created when the pipeline can not identify other types of 
 * registered job types. 
 * 
 * This is also a dummy implementation show how to create a job. 
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

/**
 * Creates the job status for an unknown job. 
 */
case class UnknownJobStatus(jobInfo:UnknownJob) extends JobStatus{
	def allCompleted():Boolean = {
       true
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("Unknown job [%s] has completed.".format(jobInfo.jobName))
    }
}


