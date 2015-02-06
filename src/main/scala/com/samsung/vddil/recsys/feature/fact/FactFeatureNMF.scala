package com.samsung.vddil.recsys.feature.fact

import scala.collection.mutable.HashMap
import scala.collection.immutable.Range
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import breeze.linalg.split
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.utils.{HashString, Logger}
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.linalg.{Vector,Vectors}
import com.samsung.vddil.recsys.feature.process.{FeaturePostProcess, FeaturePostProcessor}
import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import com.samsung.vddil.recsys.job.JobWithFeature
import com.samsung.vddil.recsys.mfmodel.ColdStartProfileGenerator
import org.apache.spark.mllib.regression.RidgeRegressionModel
import com.samsung.vddil.recsys.mfmodel.RidgeRegressionProfileGenerator
import com.samsung.vddil.recsys.feature.ItemFactorizationFeatureStruct


/*
 * Factorization Feature: Non-Negative Matrix Factorization
 */
object FactFeatureNMF  extends FeatureProcessingUnit {
    // set default parameters for matrix factorization 
    val rankStr    = "10"  // rank of the rating matrix
    val lambdaStr  = "0.1" // regularization parameter
    val numIterStr = "10"  // number of iteration to run the matrix factorization algorithm
    val debugMode = true
        
	def processFeature(
	        featureParams:HashMap[String, String],
	        jobInfo:JobWithFeature):FeatureResource = {
		
		// load training data
		val trainRatingData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get.getDataRDD()	
	    val itemMapLoc = jobInfo.jobStatus.resourceLocation_CombinedData_train.get.itemMapLoc
		val listTrainDates = jobInfo.trainDates.toList
	    val trainSource = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
		//get spark context
        val sc = jobInfo.sc
        
		// 1. Complete default parameters
        // take passed in values from featureParams or use the default parameter setting
		val rank    = featureParams.getOrElse("rank",  rankStr).toInt 
		val lambda  = featureParams.getOrElse("lambda",  lambdaStr).toDouble
		val numIter = featureParams.getOrElse("numIter",  numIterStr).toInt 
		
	    // 2. Generate resource identity using resouceIdentity()
        val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
        val resourceIden = resourceIdentity(featureParams, dataHashingStr)
        
        val userFeatureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "latentUserFeature"
        val itemFeatureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "latentItemFeature"
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "_Map_latentFeature"		
		
	    // 3. Feature generation algorithms (HDFS operations)
		// aggregate watch time on identical programs for each user and transform 
		// the data into Rating data structure  
		val trainRatingDataAgg = trainRatingData.map{
		    x => 
	        ((x._1, x._2), x._3)
		}
		.reduceByKey{_+_}
		.map(x => Rating(x._1._1, x._1._2, x._2))
		
		// apply the spark built-in function to solve matrix factorization problem
		val model = ALS.train(trainRatingDataAgg, rank, numIter, lambda)
		// get user features from the matrix factorization result
		val userFeatures = model.userFeatures
		// get item features from the matrix factorization result
		val itemFeatures = model.productFeatures
		
		
		if(jobInfo.outputResource(userFeatureFileName)){
		    userFeatures.map{
		        x => 
		        val userIDInt = x._1
		        val feature:Vector = Vectors.dense(x._2)
		        (userIDInt,feature)
		    }.saveAsObjectFile(userFeatureFileName)
		    
		    if (debugMode) {
			    userFeatures.map{
			        x => 
			        val userIDInt = x._1
			        val feature:Vector = Vectors.dense(x._2)
			        (userIDInt,feature)
			    }.saveAsTextFile(userFeatureFileName + "text")		 
		    }
		    
		    Logger.info("Saved latent user features")
		}
		
		if(jobInfo.outputResource(itemFeatureFileName)){
		    itemFeatures.map{
		        x => 
		        val itemIDInt = x._1
		        val feature:Vector = Vectors.dense(x._2)
		        (itemIDInt,feature)
		    }.saveAsObjectFile(itemFeatureFileName)
		    
		    if (debugMode) {	
			    itemFeatures.map{
			        x => 
			        val itemIDInt = x._1
			        val feature:Vector = Vectors.dense(x._2)
			        (itemIDInt,feature)
			    }.saveAsTextFile(itemFeatureFileName + "text")		
		    }
		    
		    Logger.info("Saved latent item features")
		}
		
		val indexList = Range(0,rank).toList
		val indexListRDD = sc.parallelize(indexList)
		indexListRDD.map{
		    x => 
		    (x,"latent factor number " + (x+1).toString)    
		}.saveAsTextFile(featureMapFileName)
		Logger.info("Saved latent item/user features and feature map")
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
		//TODO: Feature Selection 
		// latent item feature dimension 
		
		val itemFeatureSize = sc.objectFile[(Int, Vector)](itemFeatureFileName).first._2.size
	    val userFeatureSize = itemFeatureSize
		val featurePostProcessor:List[FeaturePostProcessor] = List()
		val itemFeatureStruct:ItemFeatureStruct = 
		    new ItemFactorizationFeatureStruct(
		            IdenPrefix, resourceIden, itemFeatureFileName, 
		            featureMapFileName, featureParams, itemFeatureSize, 
		            itemFeatureSize, featurePostProcessor, 
		            new FactFeatureNMFExtractor(itemFeatureFileName, itemMapLoc, debugMode), 
		            None)
		  
	    val userFeatureStruct:UserFeatureStruct = 
		    new UserFeatureStruct(
		            IdenPrefix, resourceIden, userFeatureFileName, 
		            featureMapFileName, featureParams, userFeatureSize, 
		            userFeatureSize, featurePostProcessor)
	
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(FeatureResource.ResourceStr_ItemFeature) = itemFeatureStruct
        resourceMap(FeatureResource.ResourceStr_UserFeature) = userFeatureStruct
        resourceMap(FeatureResource.ResourceStr_FeatureDim)  = itemFeatureSize

        
        new FeatureResource(true, Some(resourceMap), resourceIden)

	
	}
	
	val IdenPrefix:String = "FactFeatureNMF"
}


class FactFeatureNMFExtractor(
        val itemFeatureFileName:String, val itemMapLoc:String, val debugMode:Boolean) 
        extends ItemFeatureExtractor{

    protected def extractFeature(
          items:Set[String], featureSources:List[String],
          featureParams:HashMap[String, String], featureMapFileName:String,
          sc:SparkContext): RDD[(String, Vector)] = {
        
        //construct hash table from itemID to item feature, for all the training items. 
        val trainItemID2IntMap:RDD[(String, Int)] = sc.objectFile[(String, Int)](itemMapLoc)
        val trainInt2ItemIDMap:RDD[(Int, String)] = trainItemID2IntMap.map(x => (x._2,x._1))
        
        val trainItemInt2Features:RDD[(Int,Vector)] = sc.objectFile[(Int,Vector)](itemFeatureFileName)
        val trainItemID2Features = trainInt2ItemIDMap
        						   .join(trainItemInt2Features)
        						   .values
        						   
        val itemList = items.toList.map(x => (x, x))			
        val itemListRDD = sc.parallelize(itemList)
        val itemFeaturesRDD:RDD[(String, Vector)] = itemListRDD.join(trainItemID2Features).values						   
        
        if (debugMode) {
            itemFeaturesRDD.saveAsTextFile(itemFeatureFileName + "colditemtext")
        }
        
        
		itemFeaturesRDD
    }
    
    def getFeatureSources(dates:List[String], jobInfo:JobWithFeature):List[String] = {
    	dates.map{date =>
      		jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + date + "/*"
    	}.toList
    }
       
}