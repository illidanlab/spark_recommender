package com.samsung.vddil.recsys

import com.esotericsoftware.kryo.Kryo
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import scala.collection.mutable.HashMap
import scala.xml.XML
import com.samsung.vddil.recsys.job.Job
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.serializer.KryoSerializer

/**
 * This is the pipeline class, which includes pipeline configurations such as Spark Context and 
 * Hadoop file system and etc. A XML file is used to instantiate an object of pipeline class, 
 * and it is unique. 
 * 
 * The Pipeline is singleton. Use Pipeline.config to initialize the instance, 
 * and use Pipeline.instance.get to access the pipeline instance.
 */
class Pipeline private (val sc:SparkContext, val fs:FileSystem){
	val hashPartitioners:HashMap[String, HashPartitioner] = new HashMap()
	
	val kryo = new KryoSerializer(sc.getConf).newInstance
}

/**
 * A set of methods used in the pipeline, such as file system operations, spark context 
 * management (partitioner). These functions depend on an initialized Pipeline singleton 
 * object, and thus the `config` function should always be firstly invoked. 
 */
object Pipeline {
    
    /** The Singleton instance of Pipeline */
	private var Instance:Option[Pipeline] = None
	
	/** Store a list of partitioner for reuse purpose. */
	val PartitionHashDefault = "defaultHashPartitioner"
	val PartitionHashNum = "HashPartitioner%d"
	
	/** Access point of the singleton instance of Pipeline */
	def instance = Instance
	
	/**
	 * Checks if all locations in the Array exist. Returns false if not all of them 
	 * exist. This function is typically used to specify if all locations exist so 
	 * an entire block can be skipped. 
	 * 
	 * @param locArray an array of local or HDFS locations
	 * @return if all local or HDFS locations in locArray exist
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
	
	/**
	 * Returns if the resource (to be output in resourceLoc) is ready to be output. 
	 * 
	 * This function is mandatory whenever outputting a resource (combined data, features, 
	 * models). The default Spark behavior is: try to save the file no matter if current 
	 * file exists or not. And when the file exists, it throws an exception.  
	 * 
	 * This function first check if the resource location exists or not. If the file does not 
	 * exist, then the function returns true. If the file exists, depending on if we want to 
	 * overwrite it or not. Either returns false or remove the file and returns true.   
	 * 
	 * It is recommend that each job maintains a wrapped version to put 
	 * the `overwrite` variable in closure. An example is [[com.samsung.vddil.recsys.job.RecJob.outputResource]]
	 * 
	 * @param resourceLoc the full location of the resource (local or HDFS).
	 * @param overwrite if the resource should be overwrite. 
	 * @return if the resource should be output by RDD.saveTextFile or RDD.saveObject 
	 */
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
	 * Creates the singleton object Pipeline.Instance.
	 * 
	 * Initialize the instance of `com.apache.spark.SparkContext`. Some of the SparkConf options 
	 * should be passed in by outside files (such as JVM options). However, currently there is a 
	 * bug in Spark, and therefore the configuration is hard coded. 
	 * 
	 * [[http://spark.apache.org/docs/latest/configuration.html Here]] is a reference to the 
	 * available configuration for Spark. 
	 * 
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
	    	 conf.set("spark.kryoserializer.buffer.mb",   "500") //enable this if get Kryo Buffer Overflow
	    	 conf.set("spark.akka.frameSize",             "100")
             conf.set("spark.akka.timeout",               "200")
             conf.set("spark.files.userClassPathFirst",   "true")//solve dependency in httpcomponents. 
             //conf.set("spark.speculation",                "true") //this is done outside. 
	    	 
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
	 * Returns the default partitioner number for hash/range partitioner
	 * 
	 * The number is currently given by 
	 *     2 * number of executors * number of executor cores.
	 * 
	 * @return the partition number used for hash/range partitioner. 
	 */
	def getPartitionNum(trainDayNum:Int = 1):Int = {
	    require(Pipeline.instance.isDefined)
	    
	    val scConf = this.Instance.get.sc.getConf
	    
	    val (numExecutors, numExecCores)  = if (scConf.getOption("spark.master").isDefined && scConf.getOption("spark.master").get.startsWith("local")){
	        //if local then we use a small number. 
	        val numExec  = Some("3")
            val numExecC = Some("2")
            (numExec, numExecC)
	    }else{
	    	val numExec  = Pipeline.Instance.get.sc.getConf.getOption("spark.executor.instances")
	        val numExecC = Pipeline.Instance.get.sc.getConf.getOption("spark.executor.cores")
	        (numExec, numExecC)
	    }
	    
        //val numParts = 1 * numExecutors.getOrElse("100").toInt * numExecCores.getOrElse("2").toInt * (trainDayNum)
         
        val numParts = 1 * numExecutors.getOrElse("200").toInt * numExecCores.getOrElse("2").toInt       
 
        Logger.info("numParts: " + numParts)
        numParts 
	}

	
	/**
	 * Returns a cached hash partitioner given a specific partition number 
	 * 
	 * @param partitionNum the partitioner number for hash partitioner
	 */
	def getHashPartitioner(partitionNum:Int):HashPartitioner = {
	    require(Pipeline.instance.isDefined)
	    
	    val partitionerName = Pipeline.PartitionHashNum.format(partitionNum)
	    if(!Instance.get.hashPartitioners.isDefinedAt(partitionerName)){
	        val newPartitioner =  new HashPartitioner(partitionNum)
	        Instance.get.hashPartitioners(partitionerName) = newPartitioner 
	    }
	    Instance.get.hashPartitioners(partitionerName)
	}
	
    /**
     * The main entrance of the recommender system
     */
    def main(args: Array[String]): Unit = {
		PropertyConfigurator.configure("log4j.properties")
		
		// configure and create SparkContext
		config()
	
    //display spark configuratiion variables
    Instance.get.sc.getConf.getAll.map{x => Logger.info(x.toString)}


		// only proceed to jobs if pipeline is properly configured. 
		if (instance.isDefined){
		
			//Read job file
			var jobFileStr:String = "test_job.xml" 
			if (args.size > 0){
			  jobFileStr = args(0)
			  Logger.info("Job file specified: " + jobFileStr)
			}else{
			  Logger.warn("No job file specified. Used default job file: " + jobFileStr)
			}
			
			val jobList : List[Job] = 
			    Job.readFromXMLFile(jobFileStr, instance.get.sc)
			    
			//Process jobs one by one
		    Logger.info("There are " + jobList.size + " jobs read from job file.")
			for (job:Job <- jobList){
			   Logger.info("=== Running job: " + job + " ===")
			   job.run()
			   job.getStatus().showStatus()
			   Logger.info("=== Job Done: " + job + " ===")
			}
			
			Logger.info("All jobs are completed.")
		}else{
			Logger.info("Pipeline exited as configuration fails.")
		}
		
    } 

} 

/**
 * Registration of Kryo serialization
 * 
 * Currently only the following classes are registered 
 * [[com.samsung.vddil.recsys.linalg.Vector]]
 * [[com.samsung.vddil.recsys.linalg.SparseVector]]
 * [[com.samsung.vddil.recsys.linalg.DenseVector]]
 * 
 */
class SerializationRegistrator extends KryoRegistrator{
    override def registerClasses(kyro: Kryo){
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.Vector])
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.SparseVector])
        kyro.register(classOf[com.samsung.vddil.recsys.linalg.DenseVector])
        kyro.register(classOf[org.apache.spark.mllib.regression.GeneralizedLinearModel])
    }
}
