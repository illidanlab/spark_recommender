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
	def train[T](input: FeatureStruct):Option[FeaturePostProcessor]
}

object FeaturePostProcess{
    def apply(name:String, params:HashMap[String, String]):Option[FeaturePostProcess] = {
        
        name match {
            case "randomSelection"=>Some(FeatureSelection(name, params))
            case "l2normalize"    =>Some(FeatureNormalization(name, params))
            case "dummy"          =>Some(FeatureSelection(name, params))
            case _ =>None
        }
    }
}

case class FeatureNormalization(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](input: FeatureStruct):Option[FeaturePostProcessor] = {
        name match {
	        case "dummy" => DummyFeaturePostProcessor.train(input, params)
	        case _ => None
	    }
    }
}

case class FeatureSelection(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](input: FeatureStruct):Option[FeaturePostProcessor] = {
        name match {
	        case "dummy" => DummyFeaturePostProcessor.train(input, params)
	        case _ => None
	    }
    }
}

case class FeatureDimensionReduction(
        	val name:String,
        	val params:HashMap[String, String]
		) extends FeaturePostProcess{
    
    def train[T](input: FeatureStruct):Option[FeaturePostProcessor] = {
        name match {
	        case "dummy" => DummyFeaturePostProcessor.train(input, params)
	        case _ => None
	    }
    }
}

trait FeaturePostProcessor{
    
    /** Identity of the processor */
    def processorResourceId:String
    
    /** Input feature size */
    def inputFeatureSize:Int
    /** Output feature size*/
    def outputFeatureSize:Int
    
    /** Feature file name after transformation */
    def transformFeatureFile(originalFeatureFile:String): String = {
        originalFeatureFile + "_" + processorResourceId 
    } 
        
    def transformFeatureMapFile(originalFeatureMapFile:String): String = {
        originalFeatureMapFile + "_" + processorResourceId
    }
    
    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)]
    def processFeatureMap[T](trainingDataMap:RDD[(Int, String)]):RDD[(Int, String)]
    
    def process(input: FeatureStruct) : (String, String) = {
        
        val transformedFeatureMap     = processFeatureMap(input.getFeatureMapRDD)
        val transformedFeatureMapFile = transformFeatureMapFile(input.featureMapFileName)
        
        val transformedFeatureVector     = processFeatureVector(input.getFeatureRDD) 
        val transformedFeatureVectorFile = transformFeatureFile(input.featureFileName)
        
        transformedFeatureMap.saveAsTextFile(transformedFeatureMapFile)
        transformedFeatureVector.saveAsObjectFile(transformedFeatureVectorFile)
        
        (transformedFeatureMapFile, transformedFeatureVectorFile)
    }
    
    /** process a UserFeatureStruct */
    def processStruct(input: UserFeatureStruct) : UserFeatureStruct ={
        
        val (featureMapFile, featureVectorFile) = process(input: FeatureStruct)
        
        new UserFeatureStruct(
			input.featureIden, 
			input.resourceStr,
			featureVectorFile, 
			featureMapFile,
			input.featureParams,
			inputFeatureSize,
			outputFeatureSize,
			input.featurePostProcessor:+ this
        )
    }
    
    /** process a ItemFeatureStruct, returns a new one with processed feature. */
    def processStruct(input: ItemFeatureStruct) : ItemFeatureStruct = {
        //get features and transform it. 
        val (featureMapFile, featureVectorFile) = process(input: FeatureStruct)
        
        new ItemFeatureStruct(
			input.featureIden,
			input.resourceStr,
			featureVectorFile, 
			featureMapFile,
			input.featureParams,
			inputFeatureSize,
			outputFeatureSize,
			input.featurePostProcessor:+ this,
			input.extractor
		) 
    }
    
    override def toString():String = {
        s"FeatureProcessor[$processorResourceId][$inputFeatureSize => $outputFeatureSize]"
    }
}

trait FeaturePostProcessorFactory{
    /** training step */
    def train(input: FeatureStruct, params:HashMap[String, String]) : Option[FeaturePostProcessor]
}




/**
 * This is an example of implementing a post processor. 
 */
case class DummyFeaturePostProcessor(
        val inputFeatureSize:Int) extends FeaturePostProcessor{
    
    val outputFeatureSize = inputFeatureSize
    val processorResourceId = "DUM"
    
    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)] = {
        trainingData
    }
    
    def processFeatureMap[T](trainingDataMap:RDD[(Int, String)]):RDD[(Int, String)] = {
        trainingDataMap
    }

//    override def process(input: FeatureStruct) : (String, String) = {
//        val transformedFeatureMapFile = transformFeatureMapFile(input.featureMapFileName)
//        val transformedFeatureVectorFile = transformFeatureFile(input.featureFileName)
//        (transformedFeatureMapFile, transformedFeatureVectorFile)
//    }
}

object DummyFeaturePostProcessor extends FeaturePostProcessorFactory{
    def train(input: FeatureStruct, params:HashMap[String, String]) : Option[FeaturePostProcessor] = {
        //dummy feature post processor has no parameters. 
        Some(new DummyFeaturePostProcessor(input.featureSize))
    }
}
