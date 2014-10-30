/**
 *
 */
package com.samsung.vddil.recsys.feature.process

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD

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

trait FeaturePostProcessor{
    def inputFeatureSize:Int
    def outputFeatureSize:Int
    def process[T](trainingData:RDD[(T, Vector)]):RDD[(T, Vector)]
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



