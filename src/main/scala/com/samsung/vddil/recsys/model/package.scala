package com.samsung.vddil.recsys

import scala.collection.mutable.HashMap

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.evaluation.ContinuousPrediction
import com.samsung.vddil.recsys.linalg.Vector

/**
 * This package provides the handlers for model units as well as a list of regression models 
 */
package object model {
    /**
     * Parses parameter string to give parameters
     * "0:2:8,10,12" -> [0,2,4,6,8,10,12]
     */
    def parseParamString(param: String):Array[Double] = {
        param.split(",").map(_.split(":")).flatMap { _ match {
                //Array(0,2,8)
                case Array(a,b,c) => a.toDouble to c.toDouble by b.toDouble 
                //Array(10) or Array(12)
                case Array(a) => List(a.toDouble)
            }
        }
    }
    
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
            regParams = parseParamString(modelParams("regParam"))
        } else{
            modelParams("regParam") = regParams.mkString(",") 
        }
        
        if (modelParams.contains("stepSize")) {
            stepSizes = parseParamString(modelParams("stepSize"))
        } else{
            modelParams("stepSize") = stepSizes.mkString(",")
        }
        
        if (modelParams.contains("numIterations")) {
            numIterations = parseParamString(modelParams("numIterations"))
        } else{
            modelParams("numIterations") = numIterations.mkString(",") 
        }
        
        (numIterations, stepSizes, regParams)
    }
    
    /**
     * Parses data split from text file to label points
     * 
     * @deprecated 
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
    
    
}