package com.samsung.vddil.recsys.job


import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import com.samsung.vddil.recsys.Logger

/**
 * A JobStatus implementation for recommendation job. 
 * 
 */
case class RecJobStatus(jobInfo:RecJob) extends JobStatus{
	// Use the RecJob to initialize the RecJobStatus
	// so we know what are things that we want to keep track.
	
	/*
	 * Here we store the location of the resources (prepared data, features, models). 
	 */ 
	//val resourceLocation:HashMap[Any, String] = new HashMap() // a general place.
  
	var resourceLocation_CombineData:String = ""
	var resourceLocation_UserList:String = ""
	var resourceLocation_ItemList:String = ""

	val resourceLocation_AggregateData_Continuous:HashMap[String, String]       = new HashMap() 
	val resourceLocation_AggregateData_Continuous_Train:HashMap[String, String] = new HashMap() 
	val resourceLocation_AggregateData_Continuous_Test:HashMap[String, String]  = new HashMap() 
    val resourceLocation_AggregateData_Continuous_Valid:HashMap[String, String] = new HashMap() 
      
    val resourceLocation_AggregateData_Binary:HashMap[String, String]       = new HashMap() 
	val resourceLocation_AggregateData_Binary_Train:HashMap[String, String] = new HashMap() 
	val resourceLocation_AggregateData_Binary_Test:HashMap[String, String]  = new HashMap() 
    val resourceLocation_AggregateData_Binary_Valid:HashMap[String, String] = new HashMap() 
	  
    //Store user/item feature resource 
	val resourceLocation_UserFeature:HashMap[String, String] = new HashMap() 
	val resourceLocation_ItemFeature:HashMap[String, String] = new HashMap()
	//Store user/item feature map resource
	val resourceLocation_UserFeatureMap:HashMap[String, String] = new HashMap() 
	val resourceLocation_ItemFeatureMap:HashMap[String, String] = new HashMap()
	
	//Store classification/regression models
	val resourceLocation_ClassifyModel:HashMap[String, String] = new HashMap()
	val resourceLocation_RegressModel:HashMap[String, String]  = new HashMap()
	
	//Store classification/regression performance
	val resourceLocation_ClassifyPerf:HashMap[String, Any]     = new HashMap()
	val resourceLocation_RegressPerf:HashMap[String, Any]      = new HashMap()
	
	/*
	 *  As set of flags showing completed components. 
	 */
	val completedItemFeatures:HashSet[RecJobItemFeature] = new HashSet()
	val completedUserFeatures:HashSet[RecJobUserFeature] = new HashSet()
	val completedFactFeatures:HashSet[RecJobFactFeature] = new HashSet()
	val completedRegressModels:HashSet[RecJobModel] = new HashSet()
	val completedClassifyModels:HashSet[RecJobModel] = new HashSet()
	
	
	/*
	 * store persisted spark lists
	 */
	var users:Array[String] = Array[String]()
	var items:Array[String] = Array[String]()
	
    def allCompleted():Boolean = {
       true
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("Completed Item Features " + completedItemFeatures)
    	Logger.logger.info("Completed User Features " + completedItemFeatures)
    	Logger.logger.info("Completed Fact Features " + completedItemFeatures)
    	Logger.logger.info("Completed Regression Models " + completedItemFeatures)
    	Logger.logger.info("Completed Classification Models " + completedItemFeatures)
    }
} 

object RecJobStatus{
    
  
}