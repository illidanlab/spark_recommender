package com.samsung.vddil.recsys.job


import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.data.AssembledDataSet
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.feature.RecJobItemFeature
import com.samsung.vddil.recsys.feature.RecJobUserFeature
import com.samsung.vddil.recsys.feature.RecJobFactFeature
import com.samsung.vddil.recsys.model.RecJobModel
import com.samsung.vddil.recsys.testing.TestUnit
import com.samsung.vddil.recsys.data.CombinedDataSet

/** 
 * Stores the location of different types of resources (prepared data, features, models). 
 * 
 * @param jobInfo the recommendation job associated with this status. 
 */
case class RecJobStatus(jobInfo:RecJob) extends JobStatus{
    
	/*
	 * Data processing resources   
	 */ 
    var resourceLocation_CombinedData_train: Option[CombinedDataSet] = None
	var testWatchTime:Option[RDD[Rating]] = None
    
	/*
	 * Data assembling resources   
	 */ 
	val resourceLocation_AggregateData_Continuous:HashMap[String, AssembledDataSet]  = new HashMap() 
	val resourceLocation_AggregateData_Binary:    HashMap[String, AssembledDataSet]  = new HashMap() 
  
	/* 
	 * Feature extraction resources
	 */
	val resourceLocation_UserFeature:HashMap[String, FeatureStruct] = new HashMap() 
	val resourceLocation_ItemFeature:HashMap[String, FeatureStruct] = new HashMap()
	
	/*
	 * Model resources 
	 */
	val resourceLocation_ClassifyModel:HashMap[String, ModelStruct] = new HashMap()
	val resourceLocation_RegressModel:HashMap[String, ModelStruct]  = new HashMap()
	
	/*
	 * Completed components. 
	 */
	val completedItemFeatures:HashSet[RecJobItemFeature] = new HashSet()
	val completedUserFeatures:HashSet[RecJobUserFeature] = new HashSet()
	val completedFactFeatures:HashSet[RecJobFactFeature] = new HashSet()
	val completedRegressModels:HashSet[RecJobModel] = new HashSet()
	val completedClassifyModels:HashSet[RecJobModel] = new HashSet()
	val completedTests:HashMap[ModelStruct, HashMap[TestUnit, TestUnit.TestResults]] = new HashMap()
	
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
