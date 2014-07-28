package com.samsung.vddil.recsys.model.regression

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.CustomizedAlgorithm
import org.apache.spark.mllib.optimization.CustomizedModel
import org.apache.spark.mllib.regression.LabeledPoint
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.linalg.Vector

trait RegCustomizedModel[ M <: CustomizedModel] {
	/**
     * get labels and prediction on data
     */
    def getLabelAndPred(data:RDD[LabeledPoint], 
                        model:M): RDD[(Double, Double)] = {
        data.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
    }
    
    /**
     * train the passed model on training data for every combination of passed parameters
     * returns learned best model based on validation Mean Square Error
     */
    def getBestModelByValidation(trainMeth: (RDD[LabeledPoint], Int, Double, Double) => M,
    		                        trainData: RDD[LabeledPoint],
    		                        valData: RDD[LabeledPoint],
    		                        regParams: Array[Double],
    		                        stepSizes: Array[Double],
    		                        numIterations:Array[Double]):Option[M] = {
    	//build model for each parameter combination
        var bestParams:Option[(Double,Double,Double)] = None
        var bestValMSE:Option[Double] = None
        var bestModel:Option[M] = None
        for (regParam <- regParams; stepSize <- stepSizes; 
                                    numIter <- numIterations) {
            //learn model
            val model:M = trainMeth(trainData, numIter.toInt, stepSize, regParam)
            
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
    
    /**
     * Compute training and testing error.
     */
    def trainNTestError(model: M, 
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