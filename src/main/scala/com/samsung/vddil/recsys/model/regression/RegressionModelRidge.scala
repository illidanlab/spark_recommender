package com.samsung.vddil.recsys.model.regression


import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.math.stat.descriptive.moment.Mean
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.model.LinearRegressionModelStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelResource
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.model.ModelProcessingUnit
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import com.samsung.vddil.recsys.model.ModelStruct


object RegressionModelRidge extends ModelProcessingUnit {
	/*
	 * get labels and prediction on data
	 */
	def getLabelAndPred(data:RDD[LabeledPoint], 
			            model:GeneralizedLinearModel): RDD[(Double, Double)] = {
	    data.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
	}
	
	
	/*
	 * parse data split to give label point RDD
	 * 
	 */
	def parseData(dataFileName: String, sc: SparkContext):RDD[LabeledPoint]  ={
	   sc.textFile(dataFileName).map { line =>
            //(U,I,UF[],IF[], rating)
            val parts = line.split(',')
            val rating = parts(parts.length - 1).toDouble
            val features = parts.slice(2, parts.length -1).map(_.toDouble)
            LabeledPoint(rating, Vectors.dense(features))
       }
	}
	
	//models...
	def learnModel(modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob):ModelResource = {
		
		// 1. Complete default parameters 
		//TODO: put these parameter in input file
		var numIterations = 100
		var stepSize = 1.0
		var regParam = 1.0
		
		//get model parameters
		if (modelParams.contains("regParam")) {
			regParam = modelParams("regParam").toDouble
		}else{
		    modelParams("regParam") = regParam.toString() 
		}
		if (modelParams.contains("stepSize")) {
            stepSize = modelParams("stepSize").toDouble
        }else{
            modelParams("stepSize") = stepSize.toString()
        }
		if (modelParams.contains("numIterations")) {
            numIterations = modelParams("numIterations").toInt
        }else{
            modelParams("numIterations") = numIterations.toString() 
        }
	  
		
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

		//build model
		val model = RidgeRegressionWithSGD.train(trainData, numIterations, stepSize, regParam)
		
		val modelStruct:LinearRegressionModelStruct 
			= new LinearRegressionModelStruct(IdenPrefix, resourceIden, modelFileName, modelParams, model)
		
		//compute prediction on validation data
		val valLabelAndPreds = getLabelAndPred(valData, modelStruct.model)
		
		//compute error on validation
		val valMSE = ContinuousPrediction.computeMSE(valLabelAndPreds)
		
		// 4. Compute training and testing error.
		
		//compute prediction on training data
		val trLabelAndPreds = getLabelAndPred(trainData, modelStruct.model)
		//compute error on training
		val trainMSE = ContinuousPrediction.computeMSE(trLabelAndPreds)
		modelStruct.performance(ModelStruct.PerformanceTrainMSE) = trainMSE
		
		//compute prediction on test data
		val testLabelAndPreds = getLabelAndPred(testData, modelStruct.model)
		//compute error on test
		val testMSE = ContinuousPrediction.computeMSE(testLabelAndPreds)
		modelStruct.performance(ModelStruct.PerformanceTestMSE)  = testMSE
		
		
		Logger.info("trainMSE = " + trainMSE + "testMSE = " + testMSE + " valMSE = " + valMSE)
		
		modelStruct.saveModel(sc)
		
		// 5. Generate and return a ModelResource that includes all resources. 
		
	    val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(ModelResource.ResourceStr_RegressModel) = modelStruct
		
		Logger.info("Saved regression model")
		
		new ModelResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "RegModelRidge"
	
}