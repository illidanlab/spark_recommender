package com.samsung.vddil.recsys.model.regression

import com.samsung.vddil.recsys.model.ModelProcessingUnit
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelResource
import com.samsung.vddil.recsys.Logger

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object RegressionModelRidge extends ModelProcessingUnit {
	
	//models...
	def learnModel(modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob):ModelResource = {
		
		// 1. Complete default parameters 
		// 2. Generate resource identity using resouceIdentity()
		val resourceIden = resourceIdentity(modelParams, dataResourceStr)
		var modelFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobModel) + "/" + resourceIden
		//TODO: if the model is already there, we can safely return a fail. Will do that later.
		
		// 3. Model learning algorithms (HDFS operations)
		val trDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train(dataResourceStr)
		val teDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Test(dataResourceStr)
		val vaDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Valid(dataResourceStr)
		
		val sc = jobInfo.sc
		
		//TODO: compute models
		
		
		// 4. Compute training and testing error.
		
		Logger.error("TO BE IMPLEMENTED")
		
		//model.saveAsXXXFile(modelFileName)
		
		// 5. Generate and return a ModelResource that includes all resources. 
		
	    val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(ModelResource.ResourceStr_RegressModel) = modelFileName
		
		Logger.info("Saved regression model")
		
		new ModelResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "RegModelRidge"
}