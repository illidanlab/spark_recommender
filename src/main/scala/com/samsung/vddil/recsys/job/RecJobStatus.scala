package com.samsung.vddil.recsys.job


import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.data.DataSet

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
	var resourceLocation_ItemMap = ""
	var resourceLocation_UserMap = ""

	val resourceLocation_AggregateData_Continuous:HashMap[String, DataSet]  = new HashMap() 
	
	val resourceLocation_AggregateData_Continuous_Train:HashMap[String, String] = new HashMap() 
	val resourceLocation_AggregateData_Continuous_Test:HashMap[String, String]  = new HashMap() 
	val resourceLocation_AggregateData_Continuous_Valid:HashMap[String, String] = new HashMap() 
      
	val resourceLocation_AggregateData_Binary:HashMap[String, String]       = new HashMap() 
	val resourceLocation_AggregateData_Binary_Train:HashMap[String, String] = new HashMap() 
	val resourceLocation_AggregateData_Binary_Test:HashMap[String, String]  = new HashMap() 
	val resourceLocation_AggregateData_Binary_Valid:HashMap[String, String] = new HashMap() 
  
	//Store user/item feature resource
	// meta information such as user/item feature map resource are now move to FeatureStruct.
	val resourceLocation_UserFeature:HashMap[String, FeatureStruct] = new HashMap() 
	val resourceLocation_ItemFeature:HashMap[String, FeatureStruct] = new HashMap()
	
	//Store classification/regression models
	val resourceLocation_ClassifyModel:HashMap[String, ModelStruct] = new HashMap()
	val resourceLocation_RegressModel:HashMap[String, ModelStruct]  = new HashMap()
	
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
	var userIdMap:Option[Map[String, Int]] = None
	var itemIdMap:Option[Map[String, Int]] = None 
  
	/*
	 * test data in RDD[Rating] form
	 */
	var testWatchTime:Option[RDD[Rating]] = None
	
    def allCompleted():Boolean = {
       true
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("Completed Item Features " + completedItemFeatures)
    	Logger.logger.info("Completed User Features " + completedUserFeatures)
    	Logger.logger.info("Completed Fact Features " + completedFactFeatures)
    	Logger.logger.info("Completed Regression Models " + completedRegressModels)
    	Logger.logger.info("Completed Classification Models " + completedClassifyModels)
    }
} 

object RecJobStatus{
    
  
}
