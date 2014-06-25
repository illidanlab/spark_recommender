package com.samsung.vddil.recsys.model.regression


import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.samsung.vddil.recsys.model.LinearRegressionModelStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelResource
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.model.ModelProcessingUnit
import com.samsung.vddil.recsys.model.ModelStruct

object RegressionModelLasso extends ModelProcessingUnit with RegLinearModel {
	
	
	//models...
	def learnModel(modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob):ModelResource = {
		
		// 1. Complete default parameters 
		val (numIterations, stepSizes, regParams) = getParameters(modelParams)
		
		
		// 2. Generate resource identity using resouceIdentity()
		val resourceIden = resourceIdentity(modelParams, dataResourceStr)
		var modelFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobModel) + "/" + resourceIden
		
		//if the model is already there, we can safely return a fail. Will do that later.
		if (jobInfo.jobStatus.resourceLocation_RegressModel.isDefinedAt(resourceIden))
			return ModelResource.fail
		
		// 3. Model learning algorithms (HDFS operations)
		val trDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train(dataResourceStr)
		val teDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Test(dataResourceStr)
		val vaDataFilename:String = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Valid(dataResourceStr)
		
		//get the spark context
		val sc = jobInfo.sc
		
		//parse the data to get Label and feature information in LabeledPoint form
		val trainData = parseData(trDataFilename, sc)
		val testData = parseData(teDataFilename, sc)
		val valData = parseData(vaDataFilename, sc)

		//build model for each parameter combination
		var bestParams:Option[(Double,Double,Double)] = None
		var bestValMSE:Option[Double] = None
		val bestModel = getBestModelByValidation(
				                                LassoWithSGD.train, trainData, 
				                                valData, regParams, stepSizes, 
				                                numIterations)
		
		
		//save best model found above
		val modelStruct:LinearRegressionModelStruct 
			= new LinearRegressionModelStruct(IdenPrefix, resourceIden, 
					                            modelFileName, modelParams, 
					                            bestModel.get)
		
		// 4. Compute training and testing error.
		
		//compute prediction on training data
		val (trainMSE, testMSE) = trainNTestError(bestModel.get, trainData, testData)
		modelStruct.performance(ModelStruct.PerformanceTrainMSE) = trainMSE
		modelStruct.performance(ModelStruct.PerformanceTestMSE)  = testMSE
		
		Logger.info("trainMSE = " + trainMSE + "testMSE = " + testMSE 
				        + " valMSE = " + bestValMSE.get)
		
		modelStruct.saveModel(sc)
		
		// 5. Generate and return a ModelResource that includes all resources. 
		
	    val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(ModelResource.ResourceStr_RegressModel) = modelStruct
		
		Logger.info("Saved regression model")
		
		new ModelResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "RegModelLasso"
	
}