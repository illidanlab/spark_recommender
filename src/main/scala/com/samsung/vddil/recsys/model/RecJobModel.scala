package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.data.DataAssemble
import com.samsung.vddil.recsys.data.DataSplitting

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