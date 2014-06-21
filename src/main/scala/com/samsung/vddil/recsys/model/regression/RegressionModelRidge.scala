package com.samsung.vddil.recsys.model.regression

import com.samsung.vddil.recsys.model.ModelProcessingUnit
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelResource
import com.samsung.vddil.recsys.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.math.stat.descriptive.moment.Mean
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object RegressionModelRidge extends ModelProcessingUnit {
	
	var weights: Option[Vector] = None
	var intercept: Option[Double] = None

	
	/*
	 * get labels and prediction on data
	 */
	def getLabelAndPred(data:RDD[LabeledPoint], 
			            model:RidgeRegressionModel): RDD[(Double, Double)] = {
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
	
	
	/*
	 * compute mean square error
	 */
	def getMSE(labelAndPreds:RDD[(Double, Double)]): Double = {
		val (diffSum, count) = labelAndPreds.map { case(v,p) => 
			                                        //square, 1 (for count) 
                                                    (math.pow((v-p),2), 1)
                                                 }.reduce { (a, b)  =>
                                                	 //sum square, sum count
                                                    (a._1 + b._1, a._2 + b._2)
                                                 }
        diffSum/count
	}
	
	
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
		
		//get the spark context
		val sc = jobInfo.sc
		
		//parse the data to get Label and feature information in LabeledPoint form
		val trainData = parseData(trDataFilename, sc)
		val testData = parseData(teDataFilename, sc)
		val valData = parseData(vaDataFilename, sc)

		//build model
		//TODO: put these parameter in input file
		var numIterations = 100
		var stepSize = 1.0
		var regParam = 1.0
		
		//get model parameters
		if (modelParams.contains("regParam")) {
			regParam = modelParams("regParam").toDouble
		}
		if (modelParams.contains("stepSize")) {
            stepSize = modelParams("stepSize").toDouble
        }
		if (modelParams.contains("numIterations")) {
            numIterations = modelParams("numIterations").toInt
        }
		
		val model = RidgeRegressionWithSGD.train(trainData, numIterations, stepSize, regParam)
		
		
		//compute prediction on validation data
		val valLabelAndPreds = getLabelAndPred(valData, model)
		
		//compute error on validation
		val valMSE = getMSE(valLabelAndPreds)
		
		// 4. Compute training and testing error.
		
		//compute prediction on training data
		val trLabelAndPreds = getLabelAndPred(trainData, model)
		
		//compute error on training
		val trainMSE = getMSE(trLabelAndPreds)
		
		//compute prediction on test data
		val testLabelAndPreds = getLabelAndPred(testData, model)
		
		//compute error on test
		val testMSE = getMSE(testLabelAndPreds)
		
		Logger.info("trainMSE = " + trainMSE + "testMSE = " + testMSE + " valMSE = " + valMSE)
		
		weights = Some(model.weights)
		intercept = Some(model.intercept)
		
		saveModel(modelFileName, sc)
		
		// 5. Generate and return a ModelResource that includes all resources. 
		
	    val resourceMap:HashMap[String, Any] = new HashMap()
		resourceMap(ModelResource.ResourceStr_RegressModel) = modelFileName
		resourceMap(ModelResource.ResourceStr_RegressPerf) = null
		
		Logger.info("Saved regression model")
		
		new ModelResource(true, resourceMap, resourceIden)
	}
	
	val IdenPrefix:String = "RegModelRidge"
		
	def saveModel(modelFileName: String, sc: SparkContext) = {
		val hadoopConf = sc.hadoopConfiguration
		val fileSystem = FileSystem.get(hadoopConf)
		//create file on hdfs
		val out = fileSystem.create(new Path(modelFileName))

		//write weights if exists, note using Options hence first for each
		weights foreach { value =>
			value.toArray foreach {v => 
                                        out.writeChars(v.toString)  
                                        out.write(',')
			                        }
		}
		out.write('\n')
		
		//write intercept if exists
		intercept foreach {
			value => out.writeChars(value.toString)
		}
		
		//close file
		out.close()
	}
		
	def getModel(modelFileName: String, sc: SparkContext) = {
		val hadoopConf = sc.hadoopConfiguration
        val fileSystem = FileSystem.get(hadoopConf)
        
        //open file on hdfs
        val in = fileSystem.open(new Path(modelFileName))
        
        //read weights
        val weightsStr =  in.readLine().trim().split(',')
        if (weightsStr.length > 0) {
        	weights = Some(Vectors.dense(weightsStr.map(_.toDouble)))
        }
        //read intercept
        val interceptStr = in.readLine().trim()
        if (interceptStr.length() > 0) {
        	intercept = Some(interceptStr.toDouble)
        }
        
        //close file
        in.close()
        
	}
	
	
}