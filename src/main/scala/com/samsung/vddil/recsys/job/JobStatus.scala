package com.samsung.vddil.recsys.job


import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.feature.RecJobItemFeature
import com.samsung.vddil.recsys.feature.RecJobUserFeature
import com.samsung.vddil.recsys.feature.RecJobFactFeature
import com.samsung.vddil.recsys.data.CombinedDataSet

/**
 * This is the job status. The job status records if the steps are completed, and 
 * for completed steps, if they are successful or failed. The job status also notifies 
 * the job steps available resources (i.e. what are available features to build classifier).
 * 
 * An important usage of this data structure is to store the resource references that are
 * generated. And therefore this is the place where components interact with each other. 
 * See [[com.samsung.vddil.recsys.job.RecJobStatus]] for a concrete example. 
 * 
 */
trait JobStatus {
	/*
	 * This function defines if the tasks are completed. 
	 */
	def allCompleted():Boolean
	/*
	 * This function gives the status in the log file. 
	 */
	def showStatus():Unit
}

/**
 * JobStatus that includes CombinedData for training and testing. 
 */
trait JobStatusWithCombinedData extends JobStatus{
    /*
	 * Data processing resources   
	 */ 
    var resourceLocation_CombinedData_train: Option[CombinedDataSet] = None
    var resourceLocation_CombinedData_test:  Option[CombinedDataSet] = None
} 

/**
 * This supports features extraction and extracted features.
 */
trait JobStatusWithFeature extends JobStatus{

    /* 
	 * Feature extraction resources
	 */
	val resourceLocation_UserFeature:HashMap[String, FeatureStruct] = new HashMap() 
	val resourceLocation_ItemFeature:HashMap[String, FeatureStruct] = new HashMap()
	
	/*
	 * Completed components. 
	 */
	val completedItemFeatures:HashSet[RecJobItemFeature] = new HashSet()
	val completedUserFeatures:HashSet[RecJobUserFeature] = new HashSet()
	val completedFactFeatures:HashSet[RecJobFactFeature] = new HashSet()
	
	
}

