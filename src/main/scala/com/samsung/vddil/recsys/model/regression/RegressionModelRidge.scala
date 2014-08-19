package com.samsung.vddil.recsys.model.regression


import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelResource
import com.samsung.vddil.recsys.model.ModelProcessingUnit
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model._
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import com.samsung.vddil.recsys.data.AssembledDataSet

object RegressionModelRidge extends ModelProcessingUnit with RegLinearModel {
	
	
	
	//models...
	def learnModel(modelParams:HashMap[String, String], allData:AssembledDataSet, splitName:String, jobInfo:RecJob):ModelResource = {
		
		val dataResourceStr = allData.resourceStr
		
        // 1. Complete default parameters 
        val (numIterations, stepSizes, regParams) = getParameters(modelParams)
        
        
        // 2. Generate resource identity using resouceIdentity()
        val resourceIden = resourceIdentity(modelParams, dataResourceStr, splitName)
        var modelFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobModel) + "/" + resourceIden
        
        //if the model is already there, we can safely return a fail. Will do that later.
        if (jobInfo.jobStatus.resourceLocation_RegressModel.isDefinedAt(resourceIden))
            return ModelResource.fail
        
        // 3. Model learning algorithms (HDFS operations)
        val splitData = allData.getSplit(splitName).get
        val trData = splitData.training
	    val teData = splitData.testing
	    val vaData = splitData.validation
	    
        //get the spark context
        val sc = jobInfo.sc
        
        //parse the data to get Label and feature information in LabeledPoint form
        val trainData = parseDataObj(trData.resourceLoc, sc)
	    val testData  = parseDataObj(teData.resourceLoc, sc)
	    val valData   = parseDataObj(vaData.resourceLoc, sc)
        
        //build model for each parameter combination
        val bestModel = getBestModelByValidation(
                                                RidgeRegressionWithSGD.train, trainData, 
                                                valData, regParams, stepSizes, 
                                                numIterations)
        
        //save best model found above
        val modelStruct:GeneralizedLinearModelStruct 
            = new GeneralizedLinearModelStruct(IdenPrefix, resourceIden, 
            		                            dataResourceStr,
                                                modelFileName, modelParams, 
                                                bestModel.get)
        
        // 4. Compute training and testing error.
        
        //compute prediction on training data
        val (trainMSE, testMSE) = trainNTestError(bestModel.get, trainData, testData)
        modelStruct.performance(ModelStruct.PerformanceTrainMSE) = trainMSE
        modelStruct.performance(ModelStruct.PerformanceTestMSE)  = testMSE
        
        Logger.info("trainMSE = " + trainMSE + " testMSE = " + testMSE)
        Logger.info("trainData: " + trainData.count + " testData: "
                               + testData.count + " valData: " + valData.count)
        modelStruct.saveModel
        
        // 5. Generate and return a ModelResource that includes all resources. 
        
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(ModelResource.ResourceStr_RegressModel) = modelStruct
        
        Logger.info("Saved regression model")
        
        new ModelResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "RegModelRidge"
	
}