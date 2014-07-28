package com.samsung.vddil.recsys.model.regression

import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.mllib.optimization.CustomizedModel

trait RegLinearModel  {
	
    /**
     * Get labels and prediction on data
     */
    def getLabelAndPred(data:RDD[LabeledPoint], 
                        model:GeneralizedLinearModel): RDD[(Double, Double)] = {
        data.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
    }
    
    /**
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
    
    /**
     * Compute training and testing error.
     */
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
