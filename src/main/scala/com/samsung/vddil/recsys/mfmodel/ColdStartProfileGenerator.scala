package com.samsung.vddil.recsys.mfmodel

import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.RegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import scala.Array
import com.samsung.vddil.recsys.linalg.Vectors

/** used to generate cold start item or  */
trait ColdStartProfileGenerator {
    def getProfile(feature:Option[Vector] = None):Vector
}

/** This is the average profile generator, which simply provides the 
 *  average in the training data. This is used as the default cold-start 
 *  profile generator. 
 *  */
case class AverageProfileGenerator (profileRDD: RDD[Vector]) 
	extends ColdStartProfileGenerator{
    //we compute the average by default. 
    var averageVector: Option[Vector] = Some(computeAverage(profileRDD))
    
    def getProfile(feature:Option[Vector] = None):Vector = {
        if(!averageVector.isDefined){
            //set average variable. 
            averageVector = Some(computeAverage(profileRDD))
        }
        averageVector.get
    }
    
    /**
     * Compute the average of the profile vector. 
     */
    def computeAverage(profileRDD: RDD[Vector]): Vector = {
        val avgPair:(Vector, Int) = profileRDD.map{
            x=>(x, 1)
        }.reduce{ (a, b)=>
            (a._1 + b._1, a._2 + b._2)
        }
        val sumVal:Int    = avgPair._2
        
        //averaging
        if (sumVal > 0){
            avgPair._1 / sumVal.toDouble
        }else{
            avgPair._1
        }
        
    }
}

/** This is the ridge regression profile generator, which simply learns a regression
 *  model from features to each of the latent features independently. When no feature 
 *  is available, the build-in ColdStartProfileGenerator is used.  
 **/
case class RidgeRegressionProfileGenerator(
        profileRDD: RDD[(Int, Vector)], 
        contentFeatureRDD:RDD[(Int, Vector)]) extends ColdStartProfileGenerator {
    
    //latent size, determine how many regression models we need.  
    val latentDim = profileRDD.first._2.size
    val models:List[RegressionModel] = trainGenerator()
    
    val avgProfiler = AverageProfileGenerator (profileRDD.map{_._2})
    
    /**
     * Train regression model. 
     */
    def trainGenerator(): List[RegressionModel] = {
        //profileRDD: RDD[(Int, Vector)], contentFeatureRDD:RDD[(Int, Vector)]
        (0 to latentDim-1).map{ dim =>
            val trainData = profileRDD.map{x => 
                (x._1, x._2.toArray(dim))
            }.join(contentFeatureRDD).map{ x=>
                val latentFactorVal:Double = x._2._1
                val featureVect:Vector     = x._2._2
                LabeledPoint(latentFactorVal, featureVect.toMLLib)
            }
            
            val numIterations = 10
            val model = LinearRegressionWithSGD.train(trainData, numIterations)
            
            model
        }.toList
    }
    
    /**
     * Compute cold profile. 
     */
    def getProfile(feature:Option[Vector] = None):Vector = {
        if(feature.isDefined){
            
            val tt = models.map{x => x.predict(feature.get.toMLLib) }.toArray
            Vectors.dense(tt)
            
        }else{
        	avgProfiler.getProfile(None)            
        }
    }
    
}