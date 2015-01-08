package com.samsung.vddil.recsys.feature.process


import com.samsung.vddil.recsys.feature.FeatureStruct
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit

case class l2NormalizationProcessor(
        val inputFeatureSize:Int) extends FeaturePostProcessor{
    
    val outputFeatureSize = inputFeatureSize
    
    val processorResourceId = "L2N"
    
    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)] = {
        trainingData.map{line =>
            val id: T                = line._1
            val reduceVector: Vector = line._2
            (id, reduceVector.normalize(Vectors.Normal_Type_L2))
        }
    }
    
    def processFeatureMap[T](trainingDataMap:RDD[(Int, String)]):RDD[(Int, String)] = {
    	trainingDataMap
    }
}


object l2NormalizationProcessor extends FeaturePostProcessorFactory{
    def train(input: FeatureStruct, params:HashMap[String, String]) : Option[FeaturePostProcessor] = {
        //dummy feature post processor has no parameters. 
        Some(new l2NormalizationProcessor(input.featureSize))
    }    
}

case class MaxNormalizationProcessor(
        val inputFeatureSize:Int
        )extends FeaturePostProcessor{
    
    val outputFeatureSize = inputFeatureSize
    
    val processorResourceId= "MXN"
    
    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)] = {
        trainingData.map{line =>
            val id: T                = line._1
            val reduceVector: Vector = line._2
            (id, reduceVector.normalize(Vectors.Normal_Type_Max))
        }
    }
    
    def processFeatureMap[T](trainingDataMap:RDD[(Int, String)]):RDD[(Int, String)] = {
    	trainingDataMap
    }    
    
}

object MaxNormalizationProcessor extends FeaturePostProcessorFactory{
    def train(input: FeatureStruct, params:HashMap[String, String]) : Option[FeaturePostProcessor] = {
        //dummy feature post processor has no parameters. 
        Some(new MaxNormalizationProcessor(input.featureSize))
    }    
}