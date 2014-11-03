/**
 *
 */
package com.samsung.vddil.recsys.feature.process

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor

/**
 * @author jiayu.zhou
 *
 */
abstract class FeaturePostProcess {
	val name:String
	val params:HashMap[String, String]
	
	/**
	 *  From training data, train a post processor that can be used 
	 *  to transform other features. 
	 */
	def train[T](trainingData:RDD[(T, Vector)]):FeaturePostProcessor 
}

object FeaturePostProcess{
    def apply(name:String, params:HashMap[String, String]):Option[FeaturePostProcess] = {
        
        name match {
            case "randomSelection"=>Some(FeatureSelection(name, params))
            case "l2normalize"=>Some(FeatureNormalization(name, params))
            case _ =>None
        }
        
    }
}

case class FeatureNormalization(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](trainingData:RDD[(T, Vector)]):FeaturePostProcessor = {
        null
    }
}

case class FeatureSelection(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](trainingData:RDD[(T, Vector)]):FeaturePostProcessor = {
        null
    }
}

case class FeatureDimensionReduction(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](trainingData:RDD[(T, Vector)]):FeaturePostProcessor = {
        null
    }
} 

trait FeaturePostProcessor{
    
    def inputFeatureSize:Int
    def outputFeatureSize:Int
    
    def transformedFeatureFile: String
    def transformedFeatureMapFile: String
    
    def process(input: UserFeatureStruct) : UserFeatureStruct ={
        //get features and transform it. 
        val featureVector = processFeatureVector(input.getFeatureRDD) 
        val featureMap    = processFeatureMap(input.getFeatureMapRDD)
        
        featureVector.saveAsObjectFile(transformedFeatureFile)
        featureVector.saveAsTextFile(transformedFeatureMapFile)
        
        new UserFeatureStruct(
			input.featureIden, 
			input.resourceStr,
			transformedFeatureFile, 
			transformedFeatureMapFile,
			input.featureParams,
			inputFeatureSize,
			outputFeatureSize,
			input.featurePostProcessor:+ this
        )
    }
    
    def process(input: ItemFeatureStruct) : ItemFeatureStruct = {
        //get features and transform it. 
        val featureVector = processFeatureVector(input.getFeatureRDD) 
        val featureMap    = processFeatureMap(input.getFeatureMapRDD)
        
        featureVector.saveAsObjectFile(transformedFeatureFile)
        featureVector.saveAsTextFile(transformedFeatureMapFile)
        
        new ItemFeatureStruct(
			input.featureIden,
			input.resourceStr,
			transformedFeatureFile, 
			transformedFeatureMapFile,
			input.featureParams,
			inputFeatureSize,
			outputFeatureSize,
			input.featurePostProcessor:+ this,
			input.extractor
		) 
    }

    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)]
    
    def processFeatureMap[T](trainingData:RDD[(Int, String)]):RDD[(Int, String)]
    
}

case class DummyFeaturePostProcessor(
        val featureSize:Int) extends FeaturePostProcessor{
    val inputFeatureSize  = featureSize
    val outputFeatureSize = featureSize
    
    
}

