package com.samsung.vddil.recsys

import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.model.ModelUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.mllib.optimization.CustomizedModel

/**
 * This package provides the handlers for model units as well as a list of regression models 
 */
package object model {
	/**
     * Gets the parameters for optimization (SGD) based the models
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
    
    /**
     * Parses data split from text file to label points
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
    
    /**
     * Parses data split from serialized object file to label points
     */
    def parseDataObj(dataFileName: String, sc: SparkContext):RDD[LabeledPoint] = {
        //(userID:String, itemID:String, features:Vector, rating:Double)
        sc.objectFile[(Int, Int, Vector, Double)](dataFileName).map{tuple =>
            val rating:Double = tuple._4
            val feature:Vector = tuple._3
            LabeledPoint(rating, feature.toMLLib)
        }
    }
    
    
}