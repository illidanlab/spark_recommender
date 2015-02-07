package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import com.samsung.vddil.recsys.ResourceStruct
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.feature.process.FeaturePostProcessor
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.JobWithResource
import com.samsung.vddil.recsys.job.JobWithResource
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.feature.item.FactorizationItemFeatureExtractor
import com.samsung.vddil.recsys.testing._
import com.samsung.vddil.recsys.mfmodel.RidgeRegressionProfileGenerator
import com.samsung.vddil.recsys.mfmodel.LassoRegressionProfileGenerator
import com.samsung.vddil.recsys.mfmodel.AverageProfileGenerator

/**
 * This data structure stores the information of feature
 */
trait FeatureStruct extends ResourceStruct{
    
    /**
     * The identity prefix of the feature 
     */
	def featureIden:String
	def resourcePrefix = featureIden
	
	/** this is the feature size*/
	def featureSize:Int
	/** this is the original feature size */
	def featureSizeOriginal:Int
	
	/**
	 * The resource string (identity plus parameter hash)
	 */
	def resourceStr:String
	
	/**
	 * The resource location 
	 */
	def featureFileName:String
	def resourceLoc = featureFileName 
	
	/**
	 * The feature names
	 */
	def featureMapFileName:String
	
	/**
	 * Feature parameters
	 */
	def featureParams:HashMap[String, String]
	
	/**
	 * Get the RDD data structure of the content. 
	 */
	def getFeatureRDD():RDD[(Int, Vector)] = {
	    Pipeline.instance.get.sc.objectFile[(Int, Vector)](featureFileName)
	}
	
	def getFeatureMapRDD():RDD[(Int, String)] = {
	    Pipeline.instance.get.sc.textFile(featureMapFileName).map{ line =>
	        val sp = line.split(",", 2)
	        (sp(0).toInt, sp(1))
	    }
	}
	
	/** A list of processors. */
	def featurePostProcessor:List[FeaturePostProcessor]
}

object FeatureStruct{
    def saveText_featureMapRDD(rdd: RDD[(Int, String)], fileName:String, jobInfo:JobWithResource) = {
        if (jobInfo.outputResource(fileName)){
        	rdd.map{pair =>
            	pair._1.toString + "," + pair._2
        	}.saveAsTextFile(fileName)
        }
    }
    
    def saveText_featureMapRDD(rdd: => RDD[(Int, (Int, String))], fileName:String, jobInfo:JobWithResource) = {
        if (jobInfo.outputResource(fileName)){
        	rdd.map{pair =>
	            pair._1.toString + "," + pair._2._1.toString + "," + pair._2._2
	        }.saveAsTextFile(fileName)
        }
    }
}


/**
 * The data structure of user feature 
 */
case class UserFeatureStruct(
				val featureIden:String, 
				val resourceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String],
				val featureSize:Int,
				val featureSizeOriginal:Int,
				val featurePostProcessor:List[FeaturePostProcessor]
			) extends FeatureStruct {
}

/**
 * The data structure of item feature
 * 
 *  @param extractor the feature extractor used to extract features from raw 
 *         data. This can be used for extracting features for cold start items. 
 */
class ItemFeatureStruct(
				val featureIden:String,
				val resourceStr:String,
				val featureFileName:String, 
				val featureMapFileName:String,
				val featureParams:HashMap[String, String],
				val featureSize:Int,
				val featureSizeOriginal:Int,
				val featurePostProcessor:List[FeaturePostProcessor],
				val extractor:ItemFeatureExtractor,
				/** ItemFeatureStruct before current processing */
				val originalItemFeatureStruct:Option[ItemFeatureStruct]
			) extends FeatureStruct{
}

/**
 * The data structure of factorization based item feature
 * 
 * This class provides a lot of 
 */
class ItemFactorizationFeatureStruct(
				override val featureIden:String,
				override val resourceStr:String,
				override val featureFileName:String, 
				override val featureMapFileName:String,
				override val featureParams:HashMap[String, String],
				override val featureSize:Int,
				override val featureSizeOriginal:Int,
				override val featurePostProcessor:List[FeaturePostProcessor],
				val factFeatureExtractor:FactorizationItemFeatureExtractor,
				/** ItemFeatureStruct before current processing */
				override val originalItemFeatureStruct:Option[ItemFeatureStruct]
)extends ItemFeatureStruct(
        featureIden, resourceStr, featureFileName, featureMapFileName, 
        featureParams, featureSize, featureSizeOriginal, 
        featurePostProcessor, null, originalItemFeatureStruct){
    
  /**
   * Compute the factorization features for cold start items. 
   * 
   * @param coldItemContentFeatures the content features extracted for cold-start items (profiles to be computed)
   * @param featureOrderWithoutFactFeature the order of features that are not factorization features. 
   * @param factFeatureStruct the factorization-based feature to be regressed. 
   * @param sc 
   */
  def computeColdItemFactorizationFeatures(
          coldItemContentFeatures:Option[RDD[(String,Vector)]], 
          featureOrderWithoutFactFeature:List[ItemFeatureStruct], 
          coldItems:List[String],
          sc:SparkContext):RDD[(String,Vector)] = {
    
     
    // define some constants for regression method names  
    val RegressionMethod4ColdStart = "regressionMethod"	
    val RegressionMethodRidge 	   = "ridge"
    val RegressionMethodLasso 	   = "lasso"
    val RegressionMethodAverage    = "average"
    val DefaulRegressionMethod4ColdStart = RegressionMethodRidge
    val testParams = this.featureParams
    
    //get factorization feature information for training items
    val factFeatureExtractor = this.factFeatureExtractor
    val trainItemFactFeatureFileName = factFeatureExtractor.itemFeatureFileName
    val trainItemMapLoc = factFeatureExtractor.itemMapLoc
    
    // load factorization features for training items
    val trainItemID2IntMap:RDD[(String, Int)] = sc.objectFile[(String, Int)](trainItemMapLoc)
    val trainInt2ItemIDMap:RDD[(Int, String)] = trainItemID2IntMap.map(x => (x._2,x._1))   
    
    val trainItemInt2FactFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int,Vector)](trainItemFactFeatureFileName)  
    val trainItemID2FactFeaturesRDD = trainInt2ItemIDMap.join(trainItemInt2FactFeatures).map{x => (x._1, x._2._2)}  
    val trainItemFactFeaturesRDD = trainItemInt2FactFeatures.map{x => x._2} // get rid of ID and keep only the feature vector
    
    var profileGenerator = if(coldItemContentFeatures.isDefined){
	    // get content feature for training items (in order to train profile generators)
	    val trainItemContentFeatures:RDD[(Int, Vector)] = 
	        combineItemContentFeatures(featureOrderWithoutFactFeature, sc)
	    
			    //Logger.info("##2Cold items extracted: " + coldItemContentFeatures.count)
			    //Logger.info("##2content feature dim is " + trainItemContentFeatures.first._2.size)
			    
			    // create regression profile generator instance    
	    val method = testParams.getOrElseUpdate(RegressionMethod4ColdStart, RegressionMethodRidge)
	    val generator = method match{
	        case RegressionMethodRidge   => RidgeRegressionProfileGenerator(trainItemID2FactFeaturesRDD, trainItemContentFeatures)
	        case RegressionMethodLasso   => LassoRegressionProfileGenerator(trainItemID2FactFeaturesRDD, trainItemContentFeatures) 
	        case RegressionMethodAverage => AverageProfileGenerator(trainItemFactFeaturesRDD) 
	        case _                       => RidgeRegressionProfileGenerator(trainItemID2FactFeaturesRDD, trainItemContentFeatures)
	    }
	    generator
    }else{
         AverageProfileGenerator(trainItemFactFeaturesRDD)
    }
    
    //use trained profile generator to predict cold-start factorization features
    val factFeatures:RDD[(String, Vector)] = if(coldItemContentFeatures.isDefined){
	    coldItemContentFeatures.get.map{
	        item =>
	        val itemID = item._1
	        val itemContentFeature:Vector = item._2
	        val itemFactFeature:Vector = profileGenerator.getProfile(Option(itemContentFeature))
	        (itemID, itemFactFeature)
	    }
    }else{
        //if we don't have features to train then we use average for all items. 
        sc.parallelize(coldItems).map{itemStr => 
            (itemStr, profileGenerator.getProfile(None))
        }
    }

    factFeatures
  }
}

