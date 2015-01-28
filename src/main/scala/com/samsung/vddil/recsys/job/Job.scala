package com.samsung.vddil.recsys.job

import java.io.File
import java.io.InputStream
import scala.xml._
import com.samsung.vddil.recsys._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import com.samsung.vddil.recsys.utils.Logger
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.RecJobFeature
import com.samsung.vddil.recsys.feature.RecJobItemFeature
import com.samsung.vddil.recsys.feature.RecJobUserFeature
import com.samsung.vddil.recsys.feature.RecJobFactFeature
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import java.text.SimpleDateFormat
import com.samsung.vddil.recsys.utils.HashString


/**
 * Define a set of job types 
 */
object JobType extends Enumeration {
    type JobType        = Value
    val Recommendation  = Value(JobTag.JobType_Recommendation) 
    val HelloWorld      = Value(JobTag.JobType_HelloWorld)
    val TemporalAgg     = Value(JobTag.JobType_TemporalAgg)
    val RecMatrixFact   = Value(JobTag.JobType_RecMatrixFact)
    val Unknown         = Value
}

import JobType._

/** 
 * The trait of Job class. The Job serves as a data structure of jobs 
 * to be executed in the recommendation pipeline and define how the job
 * should be carried out. 
 *
 */
trait Job {
    /** The XML node for parsing information */
    def jobNode:Node
    
	/** The name of the job and will be used as a part of the file name. */
	def jobName:String
	
	/** The description of the job.  */
	def jobDesc:String
	
	/** The type of the job.  */
	def jobType:JobType
	
	/** Carries out the job execution. */
	def run():Unit
	
	def jobStatus:JobStatus
	/** Gets the current job status. */
	def getStatus():JobStatus
	
	/** Generates job XML from current information to be written in file. */
	def generateXML():Option[scala.xml.Elem]
	
    /** an instance of SparkContext created according to user specification */
    val sc:SparkContext = Pipeline.instance.get.sc
    
    /** the file system associated with sc, which can be used to operate HDFS/local FS */
    val fs:FileSystem   = Pipeline.instance.get.fs	
	
    val partitionNum_unit:Int  = Pipeline.getPartitionNum(1)
    Logger.info("Parition Number|Unit  => " + partitionNum_unit)
}




trait JobWithResource extends Job{
    
    /** 
     *  If true then the pipeline overwrites existing resources, else skip. 
     *  The flag is wrapped in [[RecJob.outputResource]] 
     */
    val overwriteResource = false //TODO: parse overwrite from job file.
    
    /**
     *  Store resource location for input/output. The input/output can be either in HDFS 
     *  or in local file system. 
     *  
	 * 	* INPUT RESOURCE
	 *     1. ROVI data folder = resourceLoc(RecJob.ResourceLoc_RoviHQ):String     
	 *     
	 *     2. ACR data folder  = resourceLoc(RecJob.ResourceLoc_WatchTime):String
	 *       
	 *  * OUTPUT RESOURCE
	 *     1. location storing features for this job    = resourceLoc(RecJob.ResourceLoc_JobFeature):String  
	 *     
	 *     2. location storing store data for this job  = resourceLoc(RecJob.ResourceLoc_JobData):String    
	 *     
	 *     3. location storing store model for this job = resourceLoc(RecJob.ResourceLoc_JobModel):String   
     */
    val resourceLoc:HashMap[String, String] = populateResourceLoc() 
    
    /** a list of addon resource locations, parsing all key value pairs from the job file */
    val resourceLocAddon:HashMap[String, String] = populateAddonResourceLoc()
    
    /**
     * Returns false if the resource is available in HDFS.
     * And therefore the Spark save MUST BE skipped. 
     * 
     * If overwriteResource is on, then this function will remove the file 
     * from HDFS, and it is thus safe to use Spark to save files. 
     * 
     * @param resourceLoc the location of the resource, e.g., 
     * 		 a HDFS file `hdfs:\\path\to\yourfile` or a local n
     *       file `\path\to\yourfile`
     */
    def outputResource(resourceLoc:String) = 
        Pipeline.outputResource(resourceLoc, overwriteResource)
        
    /**
     * Returns true if all resources are available in HDFS. 
     * And therefore the entire process logic can be skipped.
     * 
     * If overwriteResource is on, then this function returns false.
     * 
     * @param resLocArr a list of resource locations 
     */
    def skipProcessing(resLocArr:Array[String]) = 
        (!overwriteResource) && Pipeline.exists(resLocArr)
        
        /**
     * Populates special resource locations.
     * 
     * @return a map whose keys are given by 
     *    [[RecJob.ResourceLoc_RoviHQ]],
     *    [[RecJob.ResourceLoc_WatchTime]],
     *    [[RecJob.ResourceLoc_Workspace]],
     *    [[RecJob.ResourceLoc_JobData]],
     *    [[RecJob.ResourceLoc_JobFeature]],
     *    [[RecJob.ResourceLoc_JobModel]],
     *    and values are double.  
     */
    def populateResourceLoc():HashMap[String, String] = {
       var resourceLoc:HashMap[String, String] = new HashMap()
       
       var nodeList = jobNode \ JobTag.RecJobResourceLocation
       if (nodeList.size == 0){
          Logger.error("Resource locations are not given. ")
          return resourceLoc
       }
       
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).size > 0) 
    	   resourceLoc(RecJob.ResourceLoc_RoviHQ)     = (nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).text
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).size > 0) 
    	   resourceLoc(RecJob.ResourceLoc_WatchTime)  = (nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).text
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).size > 0){ 
	       resourceLoc(RecJob.ResourceLoc_Workspace)  = (nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).text
	       //derivative
	       resourceLoc(RecJob.ResourceLoc_JobDir)     = resourceLoc(RecJob.ResourceLoc_Workspace) + "/"  + jobName
	       resourceLoc(RecJob.ResourceLoc_JobData)    = resourceLoc(RecJob.ResourceLoc_JobDir) + "/data"
	       resourceLoc(RecJob.ResourceLoc_JobFeature) = resourceLoc(RecJob.ResourceLoc_JobDir) + "/feature"
	       resourceLoc(RecJob.ResourceLoc_JobModel)   = resourceLoc(RecJob.ResourceLoc_JobDir) + "/model"
	       resourceLoc(RecJob.ResourceLoc_JobTest)    = resourceLoc(RecJob.ResourceLoc_JobDir) + "/test"
	       
       }
       
       Logger.info("Resource WATCHTIME:   " + resourceLoc(RecJob.ResourceLoc_WatchTime))
       Logger.info("Resource ROVI:        " + resourceLoc(RecJob.ResourceLoc_RoviHQ))
       Logger.info("Resource Job Data:    " + resourceLoc(RecJob.ResourceLoc_JobData))
       Logger.info("Resource Job Feature: " + resourceLoc(RecJob.ResourceLoc_JobFeature))
       Logger.info("Resource Job Model:   " + resourceLoc(RecJob.ResourceLoc_JobModel))
       resourceLoc
    } 
    
    /**
     * Populates all resource locations
     * 
     * The keys are directly from the XML tags. For example in the job XML we have 
     * 
     * <resourceLocation>
			<roviHq>data/ROVI/</roviHq>
			<watchTime>data/ACR/</watchTime>
			<geoLocation>data/GeoData</geoLocation>
	 * 
	 * Gives HashMap("roviHq"->"data/ROVI/", "watchTime"->"data/ACR/", "geoLocation"->"data/GeoData")
	 * 
	 * now the geoLocation can be accessed by resourceLocAddon("geoLocation")
     */
    def populateAddonResourceLoc():HashMap[String, String] = {
        var addonResourceLoc:HashMap[String, String] = HashMap()
        
        var nodeList = jobNode \ JobTag.RecJobResourceLocation
        if (nodeList.size == 0){
           Logger.error("Resource locations are not given. ")
           return resourceLoc
        }
        
        for (resourceEntryNode <- nodeList){
          //in case multiple resource location  exist. 
          
          // the #PCDATA is currently ignored. 
          val resourceLocList = resourceEntryNode.child.
        		  map(resourceEntry => (resourceEntry.label, resourceEntry.text )).filter(_._1 != "#PCDATA")
          
          for (resourceLocPair <- resourceLocList ){
            addonResourceLoc += (resourceLocPair._1 -> resourceLocPair._2)
          }
        }
        
        //print
        Logger.info("Addon Resource Locations list:")
        addonResourceLoc.map(pair => {
	            val resourceLocKey = pair._1
	            val resourceLocVal = pair._2
	            Logger.info("Key: "+ resourceLocKey + " Value:" + resourceLocVal)
        	}
        )
        
        addonResourceLoc
    }
     
}

trait JobWithDates extends Job{
    val dateParser = new SimpleDateFormat("yyyyMMdd") // a parser/formatter for date.
    
    /** a list of dates used to generate training data/features */
    val trainDates:Array[String] = populateTrainDates()
    
    /** a list of dates used to generate testing data/features  */
    val testDates:Array[String] = populateTestDates()
    
    val partitionNum_train:Int = Pipeline.getPartitionNum(trainDates.length)
    Logger.info("Parition Number|Train => " + partitionNum_train)
    
    val partitionNum_test:Int  = Pipeline.getPartitionNum(testDates.length)
    Logger.info("Parition Number|Test  => " + partitionNum_test)
    
    /**
     * Populates training dates.
     * 
     * The dates are used to construct resource locations. The dates will be unique and sorted.
     * 
     * @return a list of date strings  
     */
    def populateTrainDates():Array[String] = {
      
      var dateList:Array[String] = Array[String]()
      
      //the element by element. 
      var nodeList = jobNode \ JobTag.RecJobTrainDateList
      if (nodeList.size == 0){
        Logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTrainDateUnit).map(_.text).
      			flatMap(expandDateList(_, dateParser)).  //expand the lists
      			toSet.toArray.sorted                     //remove duplication and sort.
      			
      Logger.info("Training dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
          
      return dateList
    }

    /**
     * Populates testing/evaluation dates.
     * 
     * The dates are used to construct resource locations. The dates will be unique and sorted.
     * 
     * @return a list of date strings  
     */
    def populateTestDates():Array[String] = {
      
      var dateList:Array[String] = Array[String]()
     
      var nodeList = jobNode \ JobTag.RecJobTestDateList
      if (nodeList.size == 0){
        Logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTestDateUnit).map(_.text).
      			flatMap(expandDateList(_, dateParser)).  //expand the lists
      			toSet.toArray.sorted                     //remove duplication and sort.
      
      Logger.info("Testing dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
          
      return dateList
    }
}

trait JobWithFeature extends JobWithResource with JobWithDates{
    
    override def jobStatus:JobStatusWithFeature
    
    /** a list of features */
    val featureList:Array[RecJobFeature] = populateFeatures()    
    
    /**
     * Populates features from XML.
     * 
     * Each feature parsed from XML is stored in a class [[RecJobFeature]]. 
     * 
     * @return a list of features required to construct 
     *         recommendation model.  
     */
    def populateFeatures():Array[RecJobFeature] = {
      
      var featList:Array[RecJobFeature] = Array()  
      
      var nodeList = jobNode \ JobTag.RecJobFeatureList 
      if (nodeList.size == 0){
        Logger.warn("No features found!")
        return featList
      } 
      
      nodeList = nodeList(0) \ JobTag.RecJobFeatureUnit 
      
      //populate each feature
      for (node <- nodeList){
        // extract feature type
        val featureType = (node \ JobTag.RecJobFeatureUnitType).text
        
        // extract feature name 
        val featureName = (node \ JobTag.RecJobFeatureUnitName).text
        
        // extract feature post-processing info
        val featureProcessList = (node \ JobTag.RecJobFeaturePostProcess).flatMap{
            node=>populateFeatureProcesses(node:Node)
        }.toList
        
        // extract feature parameters 
        val featureParam = node \ JobTag.RecJobFeatureUnitParam
        
        var paramList:HashMap[String, String] = HashMap()
        
        for (featureParamNode <- featureParam){
          //in case multiple parameter fields exist. 
          
          // the #PCDATA is currently ignored. 
          val paraPairList = featureParamNode.child.map(feat => (feat.label, feat.text )).filter(_._1 != "#PCDATA")
          
          for (paraPair <- paraPairList){
            paramList += (paraPair._1 -> paraPair._2)
          }
        } 
        
        //create feature structs by type
        featureType match{
          case JobTag.RecJobFeatureType_Item => featList = featList :+ RecJobItemFeature(featureName, paramList, featureProcessList)
          case JobTag.RecJobFeatureType_User => featList = featList :+ RecJobUserFeature(featureName, paramList, featureProcessList)
          case JobTag.RecJobFeatureType_Fact => featList = featList :+ RecJobFactFeature(featureName, paramList, featureProcessList)
          case _ => Logger.warn("Feature type %s not found and discarded.".format(featureType))
        }
        
        Logger.info("Feature found "+ featureType+ ":"+ featureName + ":" + paramList)
      }
      
      featList
    }    
    
    def populateFeatureProcesses(processNode:Node):Array[FeaturePostProcess] = {
        var processList:Array[FeaturePostProcess] = Array()
        
        var nodeList = processNode \ JobTag.RecJobFeaturePostProcessUnit
        
        for (node <- nodeList){
            val featureName = (node\JobTag.RecJobFeaturePostProcessName).text
                            
            val featureParam = node \ JobTag.RecJobFeaturePostProcessParam
    
	        var paramList:HashMap[String, String] = HashMap()
	        
	        for (featureParamNode <- featureParam){
	          //in case multiple parameter fields exist. 
	          
	          // the #PCDATA is currently ignored. 
	          val paraPairList = featureParamNode.child.map(feat => (feat.label, feat.text )).filter(_._1 != "#PCDATA")
	          
	          for (paraPair <- paraPairList){
	            paramList += (paraPair._1 -> paraPair._2)
	          }
	        }
            
            val processUnit = FeaturePostProcess(featureName, paramList)
            processUnit.foreach{unit =>
                processList = processList :+ unit
            }
        }
        
        processList
    }    
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
                case JobTag.JobType_RecMatrixFact  => RecMatrixFactJob(jobNameStr, jobDescStr, node)
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


