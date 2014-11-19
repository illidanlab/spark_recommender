/**
 *
 */
package com.samsung.vddil.recsys.feature.process

import scala.collection.mutable.HashMap

import org.apache.spark.rdd.RDD

import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
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
    

    def process(input: FeatureStruct, jobInfo:RecJob) : (String, String) = {
        
        
        val transformedFeatureMap     = processFeatureMap(input.getFeatureMapRDD)
        val transformedFeatureMapFile = transformFeatureMapFile(input.featureMapFileName)
        
        val transformedFeatureVector     = processFeatureVector(input.getFeatureRDD) 
        val transformedFeatureVectorFile = transformFeatureFile(input.featureFileName)
        
        if(jobInfo.outputResource(transformedFeatureMapFile)){
        	FeatureStruct.saveText_featureMapRDD(transformedFeatureMap, transformedFeatureMapFile, jobInfo)
        }
        if(jobInfo.outputResource(transformedFeatureVectorFile)){
        	transformedFeatureVector.saveAsObjectFile(transformedFeatureVectorFile)
        }
        
        
        (transformedFeatureMapFile, transformedFeatureVectorFile)
    }
    
    /** process a UserFeatureStruct */
    def processStruct(input: UserFeatureStruct, jobInfo:RecJob) : UserFeatureStruct ={
        
        val (featureMapFile, featureVectorFile) = process(input: FeatureStruct, jobInfo)
        
        new UserFeatureStruct(
			input.featureIden, 
			input.resourceStr,
			featureVectorFile, 
			featureMapFile,
			input.featureParams,
			outputFeatureSize,
			inputFeatureSize,
			input.featurePostProcessor:+ this
        )
    }
    
    /** process a ItemFeatureStruct, returns a new one with processed feature. */
    def processStruct(input: ItemFeatureStruct, jobInfo:RecJob) : ItemFeatureStruct = {
        //get features and transform it. 
        val (featureMapFile, featureVectorFile) = process(input: FeatureStruct, jobInfo)
        
        new ItemFeatureStruct(
			input.featureIden,
			input.resourceStr,
			featureVectorFile, 
			featureMapFile,
			input.featureParams,
			outputFeatureSize,
			inputFeatureSize,
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
 *  This is an example of implementing a post processor. It removes *the third* feature if there 
 *  are more than three features. 
 *  
 */
case class DummyFeaturePostProcessor(
        val inputFeatureSize:Int) extends FeaturePostProcessor{
    
    val performReduce = inputFeatureSize > 3
    
    //perform feature reduction only if there are more than 
    //two features. 
    val outputFeatureSize = 
        if(performReduce){  inputFeatureSize - 1
        }else{              inputFeatureSize      }
    
    val processorResourceId =
        if(performReduce){  "DUM-R"
        }else{              "DUM" }
    
    val removalPositions  = List(2)
    val inclusionPosition = ((0 until inputFeatureSize).toSet -- removalPositions.toSet).toList.sorted
    
    def processFeatureVector[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)] = {
        if(performReduce){
            trainingData.map{line =>
                val id: T                = line._1
                val reduceVector: Vector = line._2
                (id, mapFeatureVector(inclusionPosition, reduceVector))
            }
        }else{
            trainingData
        }
        
    }
    
    def processFeatureMap[T](trainingDataMap:RDD[(Int, String)]):RDD[(Int, String)] = {
        
        
        if(performReduce){
            mapFeatureMap(inputFeatureSize, inclusionPosition, trainingDataMap)
        }else{
            trainingDataMap
        }
        
    }
}

object DummyFeaturePostProcessor extends FeaturePostProcessorFactory{
    def train(input: FeatureStruct, params:HashMap[String, String]) : Option[FeaturePostProcessor] = {
        //dummy feature post processor has no parameters. 
        Some(new DummyFeaturePostProcessor(input.featureSize))
    }
}







