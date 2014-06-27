package com.samsung.vddil.recsys.model.regression

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.model.ModelUtil
import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.Logger
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm

trait RegLinearModel  {
	
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
            //features start from 3rd index
            val features = parts.slice(2, parts.length -1).map(_.toDouble)
            LabeledPoint(rating, Vectors.dense(features))
       }
    }
    
    
    /*
     * get the parameters for the model
     * (numIterations, stepSizes, regParams) default (Array(100), Array(1), Array(1))
     */
    def getParameters(modelParams:HashMap[String, String]) = {
    	// 1. Complete default parameters 
        var numIterations = Array(100.0)
        var stepSizes = Array(1.0)
        var regParams = Array(1.0)
        
        //get model parameters
        if (modelParams.contains("regParam")) {
            regParams = ModelUtil.parseParamString(modelParams("regParam"))
        } else{
            modelParams("regParam") = regParams.mkString(",") 
        }
        
        if (modelParams.contains("stepSize")) {
            stepSizes = ModelUtil.parseParamString(modelParams("stepSize"))
        } else{
            modelParams("stepSize") = stepSizes.mkString(",")
        }
        
        if (modelParams.contains("numIterations")) {
            numIterations = ModelUtil.parseParamString(modelParams("numIterations"))
        } else{
            modelParams("numIterations") = numIterations.mkString(",") 
        }
        
        (numIterations, stepSizes, regParams)
    }
    
    
    /*
     * train the passed model on training data for every combination of passed parameters
     * returns learned best model based on validation Mean Square Error
     */
    def getBestModelByValidation(trainMeth: (RDD[LabeledPoint], Int, Double, Double) => GeneralizedLinearModel,
    		                        trainData: RDD[LabeledPoint],
    		                        valData: RDD[LabeledPoint],
    		                        regParams: Array[Double],
    		                        stepSizes: Array[Double],
    		                        numIterations:Array[Double]):Option[GeneralizedLinearModel] = {
    	//build model for each parameter combination
        var bestParams:Option[(Double,Double,Double)] = None
        var bestValMSE:Option[Double] = None
        var bestModel:Option[GeneralizedLinearModel] = None
        for (regParam <- regParams; stepSize <- stepSizes; 
                                    numIter <- numIterations) {
            //learn model
            val model = trainMeth(trainData, numIter.toInt, stepSize, regParam)
            
            //perform validation
            //compute prediction on validation data
            val valLabelAndPreds = getLabelAndPred(valData, model)
        
            //compute error on validation
            val valMSE = ContinuousPrediction.computeMSE(valLabelAndPreds)
            
            Logger.info("regParam: " + regParam + " stepSize: " + stepSize 
                            + " numIter: " + numIter + " valMSE: " + valMSE)
            
            //compare and update best parameters
            if (!bestValMSE.isDefined || bestValMSE.get > valMSE) {
                bestValMSE = Some(valMSE)
                bestParams = Some(regParam, stepSize, numIter)
                bestModel = Some(model)
            }
        }
        
        bestModel
    }
    
    
    //Compute training and testing error.
    def trainNTestError(model: GeneralizedLinearModel, 
    		                trainData: RDD[LabeledPoint],
    		                testData: RDD[LabeledPoint]) = {
        //compute prediction on training data
        val trLabelAndPreds = getLabelAndPred(trainData, model)
        //compute error on training
        val trainMSE = ContinuousPrediction.computeMSE(trLabelAndPreds)
        
        //compute prediction on test data
        val testLabelAndPreds = getLabelAndPred(testData, model)
        //compute error on test
        val testMSE = ContinuousPrediction.computeMSE(testLabelAndPreds)
        
        (trainMSE, testMSE)
    }
    
    
}