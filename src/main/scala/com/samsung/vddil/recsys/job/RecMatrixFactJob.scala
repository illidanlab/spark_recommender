package com.samsung.vddil.recsys.job

import scala.xml.Node
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import com.samsung.vddil.recsys.Pipeline
import java.text.SimpleDateFormat
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.testing.RecJobTest
import com.samsung.vddil.recsys.testing.RecJobTestNoCold
import com.samsung.vddil.recsys.testing.RecJobTestColdItem
import com.samsung.vddil.recsys.evaluation.RecJobMetricMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricRMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall
import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.job._
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.mfmodel.MatrixFactModelHandler
import com.samsung.vddil.recsys.feature.RecJobFeature
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter


object RecMatrixFactJob{
	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	val ResourceLoc_JobTest    = "jobTest"
	val ResourceLoc_JobDir     = "job"
	   
	
}


case class RecMatrixFactJob(jobName:String, jobDesc:String, jobNode:Node) extends Job {
	//initialization 
    val jobType = JobType.RecMatrixFact
    
    Logger.info("Parsing job ["+ jobName + "]")
    Logger.info("        job desc:"+ jobDesc)
    
    /** an instance of SparkContext created according to user specification */
    val sc:SparkContext = Pipeline.instance.get.sc
    
    /** the file system associated with sc, which can be used to operate HDFS/local FS */
    val fs:FileSystem   = Pipeline.instance.get.fs
    
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
    
    
    val dateParser = new SimpleDateFormat("yyyyMMdd") // a parser/formatter for date. 
    
    /** a list of dates used to generate training data/features */
    val trainDates:Array[String] = populateTrainDates()
    
    val dataProcessParam:HashMap[String, String] = populateDataProcessParams()
    
    /** a list of dates used to generate testing data/features  */
    val testDates:Array[String] = populateTestDates()
    
    /** a list of features */ //TODO: parse features. 
    val featureList:Array[RecJobFeature] = new Array(1)//populateFeatures()
    
    /** a list of models */
    val modelList:Array[RecMatrixFactJobModel] = populateMethods()
    
    /** a list of test procedures to be performed for each model */
    val testList:Array[RecJobTest] = populateTests()
    
    /** A data structure maintaining resources for intermediate results. */
    val jobStatus:RecMatrixFactStatus = new RecMatrixFactStatus(this)
    
    
    val partitionNum_unit:Int  = Pipeline.getPartitionNum(1)
    Logger.info("Parition Number|Unit  => " + partitionNum_unit)
    val partitionNum_train:Int = Pipeline.getPartitionNum(trainDates.length)
    Logger.info("Parition Number|Train => " + partitionNum_train)
    val partitionNum_test:Int  = Pipeline.getPartitionNum(testDates.length)
    Logger.info("Parition Number|Test  => " + partitionNum_test)
    
    Logger.info("Job Parse done => " + this.toString)
    
    /** 
     *  Executes the main workflow of a matrix factorization-based recommender system job:
     *  
     *   1. Prepares (aggregates) training data
     *   
     *   2. Learns models. 
     *     
     *   3. Prepares testing data
     */    
    def run():Unit = {
        val logger = Logger.logger
        
        //Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	Logger.info("**preparing training data")
    	DataProcess.prepareTrain(this)
    	
    	//Prepare features in case some matrix factorization algorithms may use for cold start. 
    	//TODO: implement when necessary 
    	
    	
    	//learning models
    	if (this.modelList.length > 0){
    		  
    		Logger.info("**learning models")
	    	this.modelList.foreach{
	    	     modelUnit => {
	    	         Logger.info("*buildling model" + modelUnit.toString())
	    	         val modelOption = modelUnit.run(this)
	    	         if (modelOption.isDefined) {
	    	             this.jobStatus.resourceLocation_models = 
	    	                 this.jobStatus.resourceLocation_models + modelOption.get
	    	         }
	    	     }
	    	}
    	}
        
        //testing recommendation performance on testing dates.
    	Logger.info("**preparing testing data")
        DataProcess.prepareTest(this)
    	
        jobStatus.resourceLocation_models.map{
    	    case(modelStr, model) => 
    	        Logger.info("Evaluating model: "+ modelStr)
    	        testList.map{_.run(this, model)}
    	}
        
    	writeSummaryFile()
    	
    }
    
    def populateDataProcessParams():HashMap[String, String] = {
        var dataProcessParams = HashMap[String, String]()
	    var nodeList = jobNode \ JobTag.RecJobDataProcessMethodList
	    
	    if (nodeList.size == 0){
	        Logger.warn("No preprocessing transformation found!")
	        return dataProcessParams
	    } 
        	      
        nodeList = nodeList(0) \ JobTag.RecJobDataProcessMethodUnit       
        
        for (node <- nodeList){
        	// extract transformation method name and parameter
        	val methodName = (node \ JobTag.RecJobDataProcessMethodName).text     
        	val methodParams = (node \ JobTag.RecJobDataProcessMethodParam).text 
        	dataProcessParams += (methodName -> methodParams)
        }
        
        dataProcessParams
    }    
    
    /**
     * Generates a summary file under the job workspace folder.  
     */
    def writeSummaryFile(){
        val summaryFile = new Path(resourceLoc(RecJob.ResourceLoc_JobDir) + "/Summary.txt")
        
        //always overwrite existing summary file. 
        if (fs.exists(summaryFile)) fs.delete(summaryFile, true)
        
        val out = fs.create(summaryFile)
        val writer = new BufferedWriter(new OutputStreamWriter(out))
        
        outputSummaryFile(this, writer)

        //clean
        writer.close()
        out.close()
    }
    
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
    
    /**
     * Populates test information from XML
     * 
     * @return a set of tests to be done in the evaluation stage
     */
    def populateTests():Array[RecJobTest] = {
    	var testList:Array[RecJobTest] = Array()
    	var nodeList = jobNode \ JobTag.RecJobTestList
    	if (nodeList.size == 0){
    	    Logger.warn("No tests found!")
    	    testList
    	} else {
    		nodeList = nodeList(0) \ JobTag.RecJobTestUnit
    		
    		//populate each test
    		for (node <- nodeList) {
    			val testType = (node \ JobTag.RecJobTestUnitType).text
    			val testName = (node \ JobTag.RecJobTestUnitName).text
    			val testParam = node \ JobTag.RecJobTestUnitParam
    			var paramList:HashMap[String, String] = HashMap()
    			
    			val testMetricNode = node \ JobTag.RecJobMetricList
    			
    			// a list of test metrics to be used in test procedures 
    			val metricList:Array[RecJobMetric] = if (testMetricNode.size == 0){
    			    Array()
    			}else{
    				populateMetric(testMetricNode(0))
    				testMetricNode.flatMap{metricNode => populateMetric(metricNode)}.toArray
    			}
    			
    			//populate test parameters
    			for (param <- testParam) {
    			    val paraPairList = param.child
    			                            .map(line => (line.label, line.text))
    			                            .filter(_._1 != "#PCDATA")
    			    for (paraPair <- paraPairList) {
    			    	paramList += (paraPair._1 -> paraPair._2)
    			    }
    			}
    		
    		    //create tests by type
    			testType match {
    				case JobTag.RecJobTestType_NotCold   => 
    				    testList = testList :+ RecJobTestNoCold  (testName, paramList, metricList)
    				case JobTag.RecJobTestType_ColdItems => 
    				    testList = testList :+ RecJobTestColdItem(testName, paramList, metricList)
    				case _ => Logger.warn(s"Test type $testType not found or ignored.")
    			}
    		}
    		
    		
    	}
    	testList
    }   
    
    /**
     * Populates required evaluation metrics from XML
     * 
     * @return a set of metrics to be computed in evaluation. 
     */
    def populateMetric( node:Node ):Array[RecJobMetric] = {
    	var metricList:Array[RecJobMetric] = Array()
    	var nodeList = node \ JobTag.RecJobMetricUnit
    	if (nodeList.size == 0) {
            Logger.warn("No metrics found!")
            metricList
        } else {
        	
        	//populate each metric
        	for (node <- nodeList) {
        	    val metricType = (node \ JobTag.RecJobMetricUnitType).text
                val metricName = (node \ JobTag.RecJobMetricUnitName).text
                val metricParam = node \ JobTag.RecJobMetricUnitParam
                var paramList:HashMap[String, String] = HashMap()
                
                //populate metric parameters
                for (param <- metricParam) {
                	val paraPairList = param.child
                                            .map(line => (line.label, line.text))
                                            .filter(_._1 != "#PCDATA")
                    for (paraPair <- paraPairList) {
                        paramList += (paraPair._1 -> paraPair._2)
                    }                      
                }
        	    
        	    //create metrics by type
        	    metricType match {
        	    	case JobTag.RecJobMetricType_MSE        => metricList = metricList :+ RecJobMetricMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_RMSE       => metricList = metricList :+ RecJobMetricRMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_HR         => metricList = metricList :+ RecJobMetricHR(metricName, paramList)
        	    	case JobTag.RecJobMetricType_ColdRecall => metricList = metricList :+ RecJobMetricColdRecall(metricName, paramList)
        	    	case _ => Logger.warn(s"Metric type $metricType not found or ignored.")
        	    }
        	}
        }
    	
    	metricList
    }
    
    def generateXML(): Option[scala.xml.Elem] = None
    
    def getStatus(): com.samsung.vddil.recsys.job.JobStatus = {
        this.jobStatus
    }
    
        /**
     * Populates learning methods from XML.
     * 
     *  @return a set of models to be learned
     */
    def populateMethods():Array[RecMatrixFactJobModel] = {
      var modelList:Array[RecMatrixFactJobModel] = Array ()
      
      var nodeList = jobNode \ JobTag.RecJobModelList
      if (nodeList.size == 0){
        Logger.warn("No models found!")
        return modelList
      }
      
      nodeList = nodeList(0) \ JobTag.RecJobModelUnit
      
      //populate each model. 
      for (node <- nodeList){
         //val modelType = (node \ JobTag.RecJobModelUnitType).text
         
         val modelName = (node \ JobTag.RecJobModelUnitName).text
         
         val modelParam = node \ JobTag.RecJobModelUnitParam
         
         var paramList:HashMap[String, String] = HashMap()
         
         //populate model parameters
         for (featureParamNode <- modelParam){
           
           val paraPairList = featureParamNode.child.map(line => (line.label, line.text)).filter(_._1 != "#PCDATA")
           
           for (paraPair <- paraPairList){
             paramList += (paraPair._1 -> paraPair._2)
           }
         }
         
         //create model structures by type. 
         modelList = modelList :+ RecMatrixFactJobModel(modelName, paramList)
         
      }
      
      //TODO: if there are multiple models, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       s"Job [MatrixFact Recommendation][${this.jobName}][${this.trainDates.length} dates][${this.featureList.length} features][${this.modelList.length} models]"
    }
    
}


case class RecMatrixFactJobModel(
        val modelName:String, 
        val modelParams: HashMap[String, String]){
    def run(jobInfo:RecMatrixFactJob): Option[(String, MatrixFactModel)] = {
        
        //obtain rating data
        val ratingData:CombinedDataSet 
        	= jobInfo.jobStatus.resourceLocation_CombinedData_train.get
        
        //build model and return.  
        val modelOption = MatrixFactModelHandler.buildModel(modelName, modelParams, ratingData, jobInfo)
        
        //return results. 
        if(modelOption.isDefined){
            val model = modelOption.get
            Some((model.resourceStr, model))
        }else{
            None
        }
    }
}