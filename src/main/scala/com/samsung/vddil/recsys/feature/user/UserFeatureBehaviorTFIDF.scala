package com.samsung.vddil.recsys.feature.user

import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTFIDF
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.{Vector,Vectors,SparseVector}
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger


object UserFeatureBehaviorTFIDF extends FeatureProcessingUnit 
                                with UserFeatureItemWtAgg {
	
  def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		
    //Generate resource identity using resouceIdentity()
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		var resourceIden = resourceIdentity(featureParams,dataHashingStr)
		var featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
								    "/" + resourceIden

    generateFeature(featureParams, jobInfo, ItemFeatureSynopsisTFIDF.checkIdentity,
      featureFileName, IdenPrefix, resourceIden)

	}
	
	val IdenPrefix:String = "UserFeatureSynopsisTFIDF"
}
