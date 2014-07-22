package com.samsung.vddil.recsys

import com.esotericsoftware.kryo.Kryo
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import scala.collection.mutable.HashMap
import scala.xml.XML

import com.samsung.vddil.recsys._
import com.samsung.vddil.recsys.job._
/**
 * This is the pipeline class, which includes pipeline configurations such as Spark Context and 
 * Hadoop file system and etc. A XML file is used to instantiate an object of pipeline class, 
 * and it is unique.  
 */
class Pipeline private (val sc:SparkContext, val fs:FileSystem){
	val hashPartitioners:HashMap[String, HashPartitioner] = new HashMap()
	//val rangePartitioners:HashMap[String, RangePartitioner] = new HashMap()
}
 
object Pipeline {
    
    
    
	private var Instance:Option[Pipeline] = None
	
	/**
	 * Store a list of partitioner for reuse purpose. 
	 */

	
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
	         
	    	 val conf = new SparkConf()
	    	 conf.set("spark.executor.extraJavaOptions ", "-XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError")
	    	 conf.set("spark.serializer",                 "org.apache.spark.serializer.KryoSerializer")
	    	 conf.set("spark.kryo.registrator",           "com.samsung.vddil.recsys.SerializationRegistrator")
 
	         try{
	             //construct spark context using SparkSubmit configurations.  
	        	 sc = Some(new SparkContext(conf))
	         }catch{
	           case _:SparkException =>
	             Logger.warn("Failed to build SparkContext from Spark submit! Trying to build a local one from config file.")
	             //construct spark context based on local 
	             sc = Some(new SparkContext(conf.setMaster("local[4]").setAppName("local_test")))
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
      Logger.info("Num executors: " + numExecutors + " numExecCores: " +
        numExecCores)
      val numParts = 2 * numExecutors.getOrElse("8").toInt * numExecCores.getOrElse("2").toInt 
      numParts 
	}

	
	/**
	 * @param partitionNum the number of partitioners for hash partitioner
	 */
	def getHashPartitioner(partitionNum:Int = Pipeline.getPartitionNum):HashPartitioner = {
	    require(Pipeline.instance.isDefined)
	    
	    val partitionerName = Pipeline.PartitionHashNum.format(partitionNum)
	    if(!Instance.get.hashPartitioners.isDefinedAt(partitionerName)){
	        val newPartitioner =  new HashPartitioner(partitionNum)
	        Instance.get.hashPartitioners(partitionerName) = newPartitioner 
	    }
	    Instance.get.hashPartitioners(partitionerName)
	}

  
  def main(args: Array[String]): Unit = {
		PropertyConfigurator.configure("log4j.properties")
		
		val logger = Logger.logger
		
		//Read config file
		var cfgFileStr:String = "local_cfg.xml"
		if (args.size > 1){
		   cfgFileStr = args(1)
		   logger.info("Config file specified: " + cfgFileStr)
		}else{
		  logger.warn("No config file specified. Used default job file: " + cfgFileStr)
		}
		
		config()
		
		// only proceed to jobs if pipeline is properly configured. 
		if (instance.isDefined){
		
			//Read job file
			var jobFileStr:String = "test_job.xml" 
			if (args.size > 0){
			  jobFileStr = args(0)
			  logger.info("Job file specified: " + jobFileStr)
			}else{
			  logger.warn("No job file specified. Used default job file: " + jobFileStr)
			}
			
			//Process job file
			val jobList : List[Job] = Job.readFromXMLFile(jobFileStr,
                                                    instance.get.sc)
			
			for (job:Job <- jobList){
			   logger.info("=== Running job: " + job + " ===")
			   job.run()
			   job.getStatus().showStatus()
			   logger.info("=== Job Done: " + job + " ===")
			}
			
			logger.info("All jobs are completed.")
		}else{
			logger.info("Pipeline exited as configuration fails.")
		}
		
  } 

} 

/**
 * Registration of Kryo serialization
 * 
 * Currently only the following classes are registered 
 * 
 * com.samsung.vddil.recsys.linalg.{Vector, SparseVector, DenseVector}
 */
class SerializationRegistrator extends KryoRegistrator{
    override def registerClasses(kyro: Kryo){
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.Vector])
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.SparseVector])
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.DenseVector])
    }
}
