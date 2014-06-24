package com.samsung.vddil.recsys

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import java.io.File
import scala.xml.XML

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
	 * This function parses config XML and create the singleton object Pipeline.Instance. 
	 */
	def config(cfgFileStr: String) = {
		if(Instance.isDefined){
		    Logger.error("The pipeline is already configured. ")
		}else{
		    //do parse the xml and create the instance.
			
			Logger.info(s"Loading pipeline configuration file: $cfgFileStr")
		  
			var xml:Option[scala.xml.Elem] = None
			
			val cfgFile:File = new File(cfgFileStr)
			if (cfgFile.exists()){
			    Logger.info("Config file found in file system!") 
			    xml = Some(XML.loadFile(cfgFile))
			}else{
			    val resLoc = "/config/" + cfgFileStr
			    Logger.info(s"Config file not found in file system, try loading resource: [$resLoc]")
			    
			    try{
				    val IS = Pipeline.getClass().getResourceAsStream(resLoc)
				    xml = Some(XML.load(IS))
				    
				    Logger.info("Config file found in resource!")
				}catch{
				  case th:Throwable => Logger.error("Config file not found")
				}
			}
			
			if (xml.isDefined){
			     val nodeList = (xml.get \ CfgTag.CfgSparkContext)
			     
			     var sparkContext_master:String = CfgTag.SparkContext_master_default
			     var sparkContext_jobName:String = "RecSys_Spark"
			     
			     if ( nodeList.size > 0 && (nodeList(0) \ CfgTag.CfgSparkContextMaster).size > 0){
			    	 sparkContext_master = (nodeList(0) \ CfgTag.CfgSparkContextMaster).text
			     }else{
			         Logger.warn("SparkContext specification not found, will try using local.")
			     }
			     
			     if (nodeList.size > 0 && (nodeList(0)\CfgTag.CfgSparkContextJobName).size > 0){
			         sparkContext_jobName = (nodeList(0) \ CfgTag.CfgSparkContextJobName).text
			     }
			     
			     try{
			    	 val sc = new SparkContext(sparkContext_master, sparkContext_jobName)
			    	 val fs = FileSystem.get(sc.hadoopConfiguration)
			    	 
			    	 Instance = Some(new Pipeline(sc, fs))
			     }catch{
			       case _:Throwable => Logger.error("Failed to build SparkContext!")
			     }
			}
		}
		
	}
} 
