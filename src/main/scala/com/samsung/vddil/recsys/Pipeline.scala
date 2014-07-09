package com.samsung.vddil.recsys

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import java.io.File
import scala.xml.XML
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException

/**
 * This is the pipeline class, which includes pipeline configurations such as Spark Context and 
 * Hadoop file system and etc. A XML file is used to instantiate an object of pipeline class, 
 * and it is unique.  
 */
class Pipeline private (val sc:SparkContext, val fs:FileSystem){
	
}
 
object Pipeline {
	private var Instance:Option[Pipeline] = None
	
	def instance = Instance
	
	/**
	 * Check if all locations in the Array exist. Return false if not all of them 
	 * exist. This function is typically used to specify if all locations exist so 
	 * an entire block can be skipped. 
	 */
	def exists(locArray:Array[String]):Boolean = {
		if (!Instance.isDefined){
		   throw new IllegalAccessError("Pipeline has not been configured")
		}
		val fs = Instance.get.fs
		
		locArray.foreach(pathStr => {
				if(! fs.exists(new Path(pathStr)))
				  return false
			}
		)
		true
	}
	
	def outputResource(resourceLoc:String, overwrite:Boolean):Boolean = {
	    //proceed if Pipeline instance is ready. 
		if (!Instance.isDefined){
		   throw new IllegalAccessError("Pipeline has not been configured")
		}
	  
		val fs = Instance.get.fs
		
		val resPath = new Path(resourceLoc)
		if(fs.exists(resPath)){
		   if (overwrite){
		      fs.delete(resPath, true)
		      Logger.warn(s"Resource [$resourceLoc] is found and deleted.")
		      true
		   }else{
		      Logger.info(s"Resource [$resourceLoc] is found and output skipped.")
		      false
		   }
		}else{
		   Logger.info(s"Resource [$resourceLoc] is not found.")
		   true
		}
	}
	
	/**
	 * This function create the singleton object Pipeline.Instance. 
	 */
	def config( ) = {
		if(Instance.isDefined){
		    Logger.error("The pipeline is already configured. ")
		}else{
	       try{
	         
	    	 var sc:Option[SparkContext] = None
	         
	         try{
	        	 sc = Some(new SparkContext(new SparkConf().set("spark.executor.extraJavaOptions ", "-XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError")))
	         }catch{
	           case _:SparkException =>
	             Logger.warn("Failed to build SparkContext from Spark submit! Trying to build a local one from config file.")
	             sc = Some(new SparkContext("local[4]", "local_test"))
	         }
	         
	         if (sc.isDefined){
		         val fs = FileSystem.get(sc.get.hadoopConfiguration)
		         
		         Instance = Some(new Pipeline(sc.get, fs))
	         }
	       }catch{
	         case spEx:SparkException =>
	           	
	         case th:Throwable => 
	         	Logger.error("Failed to build SparkContext!")
	         	th.printStackTrace()
	       }
		}
	}

} 
