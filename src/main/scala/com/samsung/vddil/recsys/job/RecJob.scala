package com.samsung.vddil.recsys.job

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml._
import com.samsung.vddil.recsys.data.DataAssemble
import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.data.DataSplitting
import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.feature.FactFeatureHandler
import com.samsung.vddil.recsys.feature.ItemFeatureHandler
import com.samsung.vddil.recsys.feature.UserFeatureHandler
import com.samsung.vddil.recsys.model._
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.testing._
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger

/**
 * The constant variables of recommendation job.
 * 
 * @author jiayu.zhou
 */
object RecJob{
	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	  
	val DataSplitting_trainRatio = "trainRatio"
	val DataSplitting_testRatio  = "testRatio"
	val DataSplitting_validRatio = "validRatio"
	val DataSplitting_testRatio_default = 0.2
	val DataSplitting_validRatio_default = 0.1
	
	val SparkContext_master_default = "local[2]"
}

/**
 * The information (requirements) about a particular recommendation Job. 
 * 
 * @param jobName a name that will be display as well as construct job folder in the workspace. 
 * @param jobDesc a human readable job description 
 * @param jobNode a XML node of type scala.xml.Node, which will be used to parse all the job information.
 * 
 */
case class RecJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
	//initialization 
    val jobType = JobType.Recommendation
    
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
    
    /** a list of features */
    val featureList:Array[RecJobFeature] = populateFeatures()
    
    /** a list of models */
    val modelList:Array[RecJobModel] = populateMethods()
    
    /** a list of dates used to generate training data/features */
    val trainDates:Array[String] = populateTrainDates()
    
    /** a list of dates used to generate testing data/features  */
    val testDates:Array[String] = populateTestDates()
    
    /** a list of test procedures to be performed for each model */
    val testList:Array[RecJobTest] = populateTests()
    
    /** a list of test metrics to be used in test procedures */
    val metricList:Array[RecJobMetric] = populateMetric()
    
    /**
     * Data splitting information 
     * 
     * {{{
     * val trainRatio:Double = dataSplit(RecJob.DataSplitting_trainRatio)
     * val testRatio:Double = dataSplit(RecJob.DataSplitting_testRatio)  
     * val trainRatio:Double = dataSplit(RecJob.DataSplitting_validRatio)
     * }}} 
     */
    val dataSplit:HashMap[String, Double] = populateDataSplitting()
    //TODO: parse and ensemble 
    
    /** A data structure maintaining resources for intermediate results. */
    val jobStatus:RecJobStatus = new RecJobStatus(this)
    
    Logger.info("Job Parse done => " + this.toString)
    
    
    /** 
     *  Executes the main workflow of a recommender system job:
     *  
     *   1. Prepares (aggregates) training data
     *   
     *   2. Extracts features
     *   
     *   3. Learns models. Since each model can specify a different feature coverage threshold, 
     *                     each model learning involves an independent data assembling stage.
     *     
     *   4. Prepares testing data
     *   
     *   5. Carries out evaluations on testing data. 
     *  
     */
    def run():Unit= {
    	val logger = Logger.logger 

    	//Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	Logger.info("**preparing training data")
    	DataProcess.prepareTrain(this)
    	
    	//preparing features
    	Logger.info("**preparing features")
    	//   for each feature, we generate the resource  
    	this.featureList.foreach{
    		featureUnit =>{
    		    Logger.info("*preparing features" + featureUnit.toString())
    		    featureUnit.run(this)
    		    //status: update Job status
    		}
    	}
    	    	
    	//learning models
    	if (this.modelList.length > 0){
    		  
    		Logger.info("**learning models")
	    	this.modelList.foreach{
	    	     modelUnit => {
	    	         Logger.info("*buildling model" + modelUnit.toString())
	    	         modelUnit.run(this)
	    	     }
	    	}
    	}
    	
    	//testing recommendation performance on testing dates.
    	Logger.info("**preparing testing data")
    	DataProcess.prepareTest(this)
    	Logger.info("**evaluating the models")
    	performEvaluation()
    }
    
    /** perform evaluation of models on test data */
    def performEvaluation() {
    	
    	jobStatus.testWatchTime foreach { testData =>
    		//size of test data
    		Logger.info("Size of test data: " + testData.count)
    		
            //evaluate regression models on test data
    		jobStatus.resourceLocation_RegressModel.map{
    		    case (modelStr, model) =>
    		        testList.map{_.run(this, model, metricList)}
    		}
    		
    		//evaluate classification models on test data
    		jobStatus.resourceLocation_ClassifyModel.map{
    		    case (modelStr, model) =>
    		        //TODO: evaluate classification models. 
    		}
        }
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
     * Does nothing for the moment. 
     */
    def generateXML():Option[Elem] = {
       None
    }
    
    /**
     * Creates an instance of SparkContext
     * according to specification. 
     * 
     * @deprecated Use `Pipeline.instance.get.sc` to 
     * 		 get the instance of spark. 
     */
    def initSparkContext():Option[SparkContext]={
       var nodeList = jobNode \ JobTag.RecJobSparkContext
      
       var sparkContext_master:String  = RecJob.SparkContext_master_default
       // by default we use this.jobName as default job name of the spark context. 
       var sparkContext_jobName:String = this.jobName 
       
       if( nodeList.size > 0 && (nodeList(0) \JobTag.RecJobSparkContextMaster ).size > 0){
    	   sparkContext_master = (nodeList(0) \JobTag.RecJobSparkContextMaster).text 
       }else{
           Logger.warn("SparkContext specification not found, will try using local.")
       }
          
       if( nodeList.size > 0 && (nodeList(0) \JobTag.RecJobSparkContextJobName).size > 0){
    	   sparkContext_jobName = (nodeList(0) \JobTag.RecJobSparkContextJobName).text
       }  
   
       try{
          return Some(new SparkContext(sparkContext_master, sparkContext_jobName))
       }catch{
         case _:Throwable => Logger.error("Failed to build SparkContext!") 
       }
       
       None
    }
    
    /**
     * Populates data splitting information.
     * 
     * @return a map whose keys are given by 
     *    [[RecJob.DataSplitting_trainRatio]],
     *    [[RecJob.DataSplitting_testRatio]], and
     *    [[RecJob.DataSplitting_validRatio]]. 
     *    and values are double. 
     */
    def populateDataSplitting():HashMap[String, Double] = {
       var dataSplit:HashMap[String, Double] = new HashMap()
       
       var nodeList = jobNode \ JobTag.RecJobDataSplit
       if (nodeList.size > 0){
          //parse numbers when users have specified.
    	   if((nodeList(0) \ JobTag.RecJobDataSplitTestRatio).size > 0){
    	       try{
    	          dataSplit(RecJob.DataSplitting_testRatio) = ((nodeList(0) \ JobTag.RecJobDataSplitTestRatio).text).toDouble 
    	       }catch{ case _:Throwable => None}
    	   }
    	   
    	   if((nodeList(0) \ JobTag.RecJobDataSplitValidRatio).size > 0){
    	       try{
    	          dataSplit(RecJob.DataSplitting_validRatio) = ((nodeList(0) \ JobTag.RecJobDataSplitValidRatio).text).toDouble 
    	       }catch{ case _:Throwable => None}
    	   }
       }else{
          Logger.warn("Data splitting is not specified for job [%s]".format(jobName))
       }
       
       //use default if users have not specified we use default. 
       if(! dataSplit.isDefinedAt(JobTag.RecJobDataSplitTestRatio)) 
           dataSplit(RecJob.DataSplitting_testRatio) = RecJob.DataSplitting_testRatio_default
       
       if(! dataSplit.isDefinedAt(JobTag.RecJobDataSplitValidRatio))
           dataSplit(RecJob.DataSplitting_validRatio) = RecJob.DataSplitting_validRatio_default
       
       dataSplit(RecJob.DataSplitting_trainRatio) 
       	   = 1.0 - dataSplit(RecJob.DataSplitting_validRatio) - dataSplit(RecJob.DataSplitting_testRatio)
           
       dataSplit
    }
    
    /**
     * Populates resource locations.
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
	       resourceLoc(RecJob.ResourceLoc_JobData)    = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/data"
	       resourceLoc(RecJob.ResourceLoc_JobFeature) = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/feature"
	       resourceLoc(RecJob.ResourceLoc_JobModel)   = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/model"
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
     * The dates are used to construct resource locations. The dates are unsorted.
     * 
     * @return a list of date strings  
     */
    def populateTrainDates():Array[String] = {
      
      var dateList:Seq[String] = Seq()
     
      var nodeList = jobNode \ JobTag.RecJobTrainDateList
      if (nodeList.size == 0){
        Logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTrainDateUnit).map(_.text)
      
      Logger.info("Training dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
      return dateList.toArray
    }
    
    
    /**
     * Populates testing/evaluation dates.
     * 
     * The dates are used to construct resource locations. The dates are unsorted.
     * 
     * @return a list of date strings  
     */
    def populateTestDates():Array[String] = {
      
      var dateList:Seq[String] = Seq()
     
      var nodeList = jobNode \ JobTag.RecJobTestDateList
      if (nodeList.size == 0){
        Logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTestDateUnit).map(_.text)
      
      Logger.info("Testing dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
      return dateList.toArray
    }
    
    
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
        
        // extract features 
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
          case JobTag.RecJobFeatureType_Item => featList = featList :+ RecJobItemFeature(featureName, paramList)
          case JobTag.RecJobFeatureType_User => featList = featList :+ RecJobUserFeature(featureName, paramList)
          case JobTag.RecJobFeatureType_Fact => featList = featList :+ RecJobFactFeature(featureName, paramList)
          case _ => Logger.warn("Feature type %s not found and discarded.".format(featureType))
        }
        
        Logger.info("Feature found "+ featureType+ ":"+ featureName + ":" + paramList)
      }
      
      featList
    }
    
    
    /**
     * Populates required evaluation metrics from XML
     * 
     * @return a set of metrics to be computed in evaluation. 
     */
    def populateMetric():Array[RecJobMetric] = {
    	var metricList:Array[RecJobMetric] = Array()
    	var nodeList = jobNode \ JobTag.RecJobMetricList
    	if (nodeList.size == 0) {
            Logger.warn("No metrics found!")
            metricList
        } else {
        	nodeList = nodeList(0) \ JobTag.RecJobMetricUnit
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
        	    	case JobTag.RecJobMetricType_MSE => metricList = metricList :+ RecJobMetricMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_RMSE => metricList = metricList :+ RecJobMetricRMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_HR => metricList = metricList :+ RecJobMetricHR(metricName, paramList)
        	    	case _ => Logger.warn("Metric type $metricType not found or ignored.")
        	    }
        	}
        }
    	
    	metricList
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
    				case JobTag.RecJobTestType_NotCold => testList = testList :+ RecJobTestNoCold(testName, paramList)
    				case _ => Logger.warn("Test type $testType not found or ignored.")
    			}
    			
    			
    		}
    	}
    	
    	testList
    }
    
    
    /**
     * Populates learning methods from XML.
     * 
     *  @return a set of models to be learned
     */
    def populateMethods():Array[RecJobModel] = {
      var modelList:Array[RecJobModel] = Array ()
      
      var nodeList = jobNode \ JobTag.RecJobModelList
      if (nodeList.size == 0){
        Logger.warn("No models found!")
        return modelList
      }
      
      nodeList = nodeList(0) \ JobTag.RecJobModelUnit
      
      //populate each model. 
      for (node <- nodeList){
         val modelType = (node \ JobTag.RecJobModelUnitType).text
         
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
         
         //create model structs by type. 
         modelType match{
           case JobTag.RecJobModelType_Regress => modelList = modelList :+ RecJobScoreRegModel(modelName, paramList)
           case JobTag.RecJobModelType_Classify => modelList = modelList :+ RecJobBinClassModel(modelName, paramList)
           case _ => Logger.warn("Model type $modelType not found and ignored.")
         }
      }
      
      //TODO: if there are multiple models, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       s"Job [Recommendation][${this.jobName}][${this.trainDates.length} dates][${this.featureList.length} features][${this.modelList.length} models]"
    }
    
    /**
     * Return the current job status. 
     */
    def getStatus():JobStatus = {
       return this.jobStatus
    }
    
}





/**
 * The recommendation job feature data structure.  
 * 
 * {{{
 * new RecJobUserFeature("Zapping", (freq -> 10)) 
 * new RecJobFactFeature("PMF", (k -> 10, pass -> 1))
 * }}}
 */
sealed trait RecJobFeature{
    /** the feature name used to invoke different feature extraction algorithm */
    def featureName:String
    
    /** feature extraction parameters */
    def featureParams:HashMap[String, String]
    
    /** Extracts features and store (extracted) feature information in jobStatus */
	def run(jobInfo: RecJob):Unit
}

/** Item feature (program feature) e.g., genre  */
case class RecJobItemFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	   jobInfo.jobStatus.completedItemFeatures(this) = ItemFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}

/** User feature e.g., watch time, zapping */
case class RecJobUserFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	   jobInfo.jobStatus.completedUserFeatures(this) = UserFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}

/** Factorization-based (collaboration filtering) features. */
case class RecJobFactFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	    jobInfo.jobStatus.completedFactFeatures(this) = FactFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}


/**
 * Defines the type of metric to be used in evaluation
 */
sealed trait RecJobMetric{
    /** Name of the metric */
    def metricName: String
    
    /** Parameters of the metric */
    def metricParams: HashMap[String, String]
}

/** Generic metric type of squared error */
trait RecJobMetricSE extends RecJobMetric {
	def run(labelNPred: RDD[(Double, Double)]): Double
}

/** Metric type of hit rate */
case class RecJobMetricHR(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetric {
	//will calculate average hit rate across passed user hits for all items
    // and new items 
    def run(hitSets:RDD[HitSet]):(Double, Double) = {
    	println(hitSets.count)
        //TODO:  case when there was no new item in test 
        val hitScores = hitSets.map {hitSet =>
            val allHRInters = (hitSet.topNPredAllItem.toSet & 
                                   hitSet.topNTestAllItems.toSet).size.toDouble
            val newHRInters = (hitSet.topNPredNewItems.toSet & 
                                   hitSet.topNTestNewItems.toSet).size.toDouble
            (allHRInters/hitSet.N, newHRInters/hitSet.N, 1)
        }.reduce((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3))
        val numUsers = hitScores._3
        val avgHitRate = (hitScores._1/numUsers, hitScores._2/numUsers)
        avgHitRate
    }
}

/** Metric type of mean squared error */
case class RecJobMetricMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
	def run(labelNPred: RDD[(Double, Double)]): Double = {
		//NOTE: can be further extended to use metricName and metricParams like test and model
		ContinuousPrediction.computeMSE(labelNPred)
	}
}

/** Metric type of root mean squared error */
case class RecJobMetricRMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
    def run(labelNPred: RDD[(Double, Double)]): Double = {
        //NOTE: can be further extended to use metricName and metricParams like test and model
        ContinuousPrediction.computeRMSE(labelNPred)
    }
}


/** Defines the type of test. */
sealed trait RecJobTest {
    /** The name of the test */
    def testName: String
    
    /** The parameters of the test */
    def testParams: HashMap[String, String]
	
    /**
     * Runs model on test data and returns results in the form 
     * of (user, item, actual label, predicted label)
     * 
     * @return RDD of (user, item, actual label, predicted label)
     */
	def run(jobInfo: RecJob, model:ModelStruct, metricList:Array[RecJobMetric])
}

/** Non cold start evaluation */
case class RecJobTestNoCold(testName: String, testParams: HashMap[String, String]) 
    extends RecJobTest {
	
	var testHandlerRes:Option[RDD[(Int, Int, Double, Double)]] = None
	var hitTestHandlerRes:Option[RDD[HitSet]] = None
	
	/*
	 * run model on test data and return RDD of (user, item, actual label, predicted label)
	 */
	def run(jobInfo: RecJob, model:ModelStruct, metricList:Array[RecJobMetric]) = {
		
		model match {
			//get predicted labels
			case linearModel:LinearRegressionModelStruct => {
			    metricList.map { metric =>
			    	metric match {
			    		case metricSE:RecJobMetricSE => {
			    			linearModelSEEval(jobInfo, linearModel)
			    			testHandlerRes foreach { testVal =>
		    				     val score = metricSE.run(testVal.map{x => (x._3, x._4)})
		    				     //TODO: add test type
		    				     Logger.info(s"Evaluated $model Not Coldstart $metric = $score")
			    				
			    			}
			    		}
			    		
			    		case metricHR:RecJobMetricHR => {
			    			linearModelHREval(jobInfo, linearModel)
			    			hitTestHandlerRes foreach { testVal =>
			    				val scores  = metricHR.run(testVal)
			    				//TODO: add test type
                                 Logger.info(s"Evaluated $model Not Coldstart  $metric = $scores")
			    			}
			    		}
			    		
			    		case _ => Logger.warn(s"$metric not known metric")
			    	}
			    }
			}
			case _ => None
		}
	}
	
	def linearModelSEEval(jobInfo: RecJob, 
			                linearModel:LinearRegressionModelStruct) = {
		if (!testHandlerRes.isDefined) {
            testHandlerRes = Some(LinearRegNotColdTestHandler.performTest(jobInfo, 
            		                          testName, testParams, linearModel))
        }
	}	
	
	def linearModelHREval(jobInfo: RecJob, 
			                linearModel:LinearRegressionModelStruct) = {
		if (!hitTestHandlerRes.isDefined) {
			hitTestHandlerRes = Some(RegNotColdHitTestHandler.performTest(jobInfo, 
					                    testName, testParams, linearModel))
		}
	}
	
}

/** The learning to rank models */
sealed trait RecJobModel{
    
    /** name of the model */
    def modelName:String
    
    /** model parameters */
    def modelParams:HashMap[String, String]
    
    /** build models */
	def run(jobInfo: RecJob):Unit
}

/** Constants used in building models. */
object RecJobModel{
	val defaultMinUserFeatureCoverage = 0.3
	val defaultMinItemFeatureCoverage = 0.5
  
	val Param_MinUserFeatureCoverage = "minUFCoverage"
	val Param_MinItemFeatureCoverage = "minIFCoverage"
}

/** Regression model */
case class RecJobScoreRegModel(modelName:String, modelParams:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with continuous labels (X, y). 
		Logger.info("**assembling data")
    	
		var minIFCoverage = RecJobModel.defaultMinItemFeatureCoverage
		var minUFCoverage = RecJobModel.defaultMinUserFeatureCoverage
		
		//TODO: parse from XML
		
		val dataResourceStr = DataAssemble.assembleContinuousData(jobInfo, minIFCoverage, minUFCoverage)
		
		//2. divide training, testing, validation
		Logger.info("**divide training/testing/validation")
		DataSplitting.splitContinuousData(jobInfo, dataResourceStr, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio)
		)
		
	    //3. train model on training and tune using validation, and testing.
		Logger.info("**building and testing models")
		
		jobInfo.jobStatus.completedRegressModels(this) = 
		    RegressionModelHandler.buildModel(modelName, modelParams, dataResourceStr, jobInfo) 
	}
}

/** Classification model */
case class RecJobBinClassModel(modelName:String, modelParams:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with binary labels (X, y)
	   Logger.info("**assembling binary data")  
	   
	   var minIFCoverage = RecJobModel.defaultMinItemFeatureCoverage
	   var minUFCoverage = RecJobModel.defaultMinUserFeatureCoverage
	   
	   var balanceTraining = false
	   
	   //TODO: parse from XML
	   
	   val dataResourceStr = DataAssemble.assembleBinaryData(jobInfo, minIFCoverage, minUFCoverage)
	   
	   //2. divide training, testing, validation
	   Logger.info("**divide training/testing/validation")
	   DataSplitting.splitBinaryData(jobInfo, dataResourceStr, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio),
		    balanceTraining
		)
	   
	   //3. train model on training and tune using validation, and testing.
	   Logger.info("**building and testing models")
	   
	   jobInfo.jobStatus.completedClassifyModels(this) = 
	       ClassificationModelHandler.buildModel(modelName, modelParams, dataResourceStr, jobInfo)
       
	}
}