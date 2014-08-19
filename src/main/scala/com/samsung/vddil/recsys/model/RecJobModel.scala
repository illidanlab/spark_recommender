package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.data.DataAssemble
import com.samsung.vddil.recsys.data.DataSplitting
import com.samsung.vddil.recsys.data.AssembledDataSet

/** 
 *  The learning to rank models
 *  @param modelName name of the model
 *  @param modelParams model parameters
 */
abstract class RecJobModel(val modelName:String, val modelParams: HashMap[String, String]){
    
    var minIFCoverage = modelParams.getOrElseUpdate(
		        	RecJobModel.Param_MinItemFeatureCoverage, 
		        	RecJobModel.defaultMinItemFeatureCoverage).toDouble
		        	
	var minUFCoverage = modelParams.getOrElseUpdate(
		        	RecJobModel.Param_MinUserFeatureCoverage, 
		        	RecJobModel.defaultMinUserFeatureCoverage).toDouble
    
    /** build models */
	def run(jobInfo: RecJob):Unit
}

/** Constants used in building models. */
object RecJobModel{
	val defaultMinUserFeatureCoverage = "0.1"
	val defaultMinItemFeatureCoverage = "0.1"
  
	val Param_MinUserFeatureCoverage = "minUFCoverage"
	val Param_MinItemFeatureCoverage = "minIFCoverage"
}

/** Regression model */
case class RecJobScoreRegModel(
        override val modelName:String, 
        override val modelParams:HashMap[String, String]) 
	extends RecJobModel(modelName, modelParams){
    
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with continuous labels (X, y). 
		Logger.info("**assembling data")
    	
		val allData:AssembledDataSet = 
		    DataAssemble.assembleContinuousData(jobInfo, minIFCoverage, minUFCoverage)
		
		//2. divide training, testing, validation
		Logger.info("**divide training/testing/validation")
		val splitName = DataSplitting.splitContinuousData(jobInfo, allData, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio)
		)
		
	    //3. train model on training and tune using validation, and testing.
		Logger.info("**building and testing models")
		
		jobInfo.jobStatus.completedRegressModels(this) = 
		    RegressionModelHandler.buildModel(modelName, modelParams, allData, splitName, jobInfo) 
	}
}

/** Classification model */
case class RecJobBinClassModel(
        override val modelName:String, 
        override val modelParams:HashMap[String, String]) 
	extends RecJobModel(modelName, modelParams){
    
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with binary labels (X, y)
	   Logger.info("**assembling binary data")  
	   
	   var balanceTraining = false
	   
	   val allData = DataAssemble.assembleBinaryData(jobInfo, minIFCoverage, minUFCoverage)
	   
	   //2. divide training, testing, validation
	   Logger.info("**divide training/testing/validation")
	   val splitName = DataSplitting.splitBinaryData(jobInfo, allData, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio),
		    balanceTraining
		)
	   
	   //3. train model on training and tune using validation, and testing.
	   Logger.info("**building and testing models")
	   
	   jobInfo.jobStatus.completedClassifyModels(this) = 
	       ClassificationModelHandler.buildModel(modelName, modelParams, allData, splitName, jobInfo)
       
	}
}