/**
 * This file includes a set of job descriptions. 
 */
package com.samsung.vddil.recsys.job

import java.io.File
import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}
import org.w3c.dom.{Document, Element, Node, NodeList}

import com.samsung.vddil.recsys._

/*
 * Define a set of types 
 */
object JobType extends Enumeration {
     	type JobType = Value
     	val Recommendation = Value("recommendation") 
     	val HelloWorld = Value("helloworld")
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
	    val dbFactory:DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
	    val dBuilder:DocumentBuilder = dbFactory.newDocumentBuilder()
	    val doc:Document = dBuilder.parse(jobfile)
	    doc.getDocumentElement().normalize()
	    
	    if (doc == null){
	      logger.warn("Failed to parse XML document!")
	      return jobList
	    }
	    
	    val nodes:NodeList = doc.getElementsByTagName("jobEntry")
	    
	    
	    var i = 0
	    for (i <- 0 to nodes.getLength() -1 ){
	       val node:Node = nodes.item(i) 
	       if (node.getNodeType() == Node.ELEMENT_NODE){
	           val jobTypeStr:String = getValue(node.asInstanceOf[Element], "jobType")
	           val jobNameStr:String = getValue(node.asInstanceOf[Element], "jobName")
	           val jobDescStr:String = getValue(node.asInstanceOf[Element], "jobDesc")
	           
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
	    }
	     
	    logger.info("There are ["+ jobList.length + "] Jobs found. ")
	  }else{
	    logger.warn("Job file not found!")
	  }
	  
	  jobList
	}
	
	/*
	 * Auxiliary XML function. 
	 */
	def getValue(elem:Element, tag:String):String = {
	   val tagElem = elem.getElementsByTagName(tag) 
	   if (tagElem.getLength() <= 0){
	      return "n/a"
 	   }
	   val nodes:NodeList = tagElem.item(0).getChildNodes()
	   val node:Node = nodes.item(0).asInstanceOf[Node]
	   node.getNodeValue()
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


