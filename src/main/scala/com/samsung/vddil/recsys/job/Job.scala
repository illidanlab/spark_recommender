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
     	val Recommendation, HelloWorld, Unknown = Value
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
	
}

object Job{
	/*
	 * Read from 
	 */
	def readFromXMLFile(filename:String):Array[Job] = {
	  
	  val logger = Logger
	  logger.info("Loading XML file: " + filename)
	  
	  val jobfile:File = new File(filename)
	  
	  if (jobfile.exists()) {
	      
	  }
	  
	  return null
	}
}


case class RecJob (jobName:String, jobDesc:String, jobType:JobType) extends Job{
	
}




