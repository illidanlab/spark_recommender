package com.samsung.vddil.recsys

import java.io.File
import scala.xml.XML
import scala.collection.mutable.HashMap
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner


/**
 * This is the pipeline class, which includes pipeline configurations such as Spark Context and 
 * Hadoop file system and etc. A XML file is used to instantiate an object of pipeline class, 
 * and it is unique.  
 */
class Pipeline private (val sc:SparkContext, val fs:FileSystem){
	
}
 
object Pipeline {
	private var Instance:Option[Pipeline] = None
	
	/**
	 * Store a list of partitioner for reuser purpose. 
	 */
	private var partitioners:HashMap[String, Partitioner] = new HashMap() 
	
	val PartitionHashDefault = "defaultHashPartitioner"
	val PartitionHashNum = "HashPartitioner%d"
	
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
	
	/**
	 * Get the default number for hash partitioner 
	 */
	def getPartitionNum():Int = {
	    require(Pipeline.instance.isDefined)
	    
	    val numExecutors = Pipeline.Instance.get.sc.getConf.getOption("spark.executor.instances")
        val numExecCores = Pipeline.Instance.get.sc.getConf.getOption("spark.executor.cores")
        2 * numExecutors.getOrElse("8").toInt * numExecCores.getOrElse("2").toInt
	}

	
	/**
	 * @param partitionNum the number of partitioners for hash partitioner
	 */
	def getHashPartitioner(partitionNum:Int = Pipeline.getPartitionNum):HashPartitioner = {
	    require(Pipeline.instance.isDefined)
	    
	    val partitionerName = Pipeline.PartitionHashNum.format(partitionNum)
	    if(!this.partitioners.isDefinedAt(partitionerName)){
	        val newPartitioner =  new HashPartitioner(partitionNum)
	        this.partitioners(partitionerName) = newPartitioner 
	    }
	    this.partitioners(partitionerName).asInstanceOf[HashPartitioner]
	}
} 
