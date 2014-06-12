package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.fact.FactFeatureNMF
import com.samsung.vddil.recsys.feature.fact.FactFeaturePMF

/*
 * This is the main entrance of the factorization feature processing.
 * 
 * TODO: change this component to dynamic class loading.
 */
object FactFeatureHandler {
	val FFNMF = "nmf"
	val FFPMF = "pmf"
	
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean = {
		Logger.logger.info("Processing factorization feature [%s:%s]".format(featureName, featureParams))
		 
		//Process the features accordingly 
		var resource:FeatureResource = new FeatureResource(false)
		
		featureName match{
		  case FFNMF => resource = FactFeatureNMF.processFeature(featureParams, jobInfo)
		  case FFPMF => resource = FactFeaturePMF.processFeature(featureParams, jobInfo)
		  case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
		}
		
		if(resource.success){
		  //upon success, the Fact feature handler generates a user feature and an item feature.
		  
		   resource.resourceMap(FeatureResource.ResourceStr_ItemFeature) match{
		      case resourceStr:String => 
		        jobInfo.jobStatus.resourceLocation_ItemFeature("blah") = resourceStr
		   }
		   
		   resource.resourceMap(FeatureResource.ResourceStr_UserFeature) match{
		      case resourceStr:String => 
		        jobInfo.jobStatus.resourceLocation_UserFeature("blah") = resourceStr
		   }
		}
		
		resource.success
	}
}