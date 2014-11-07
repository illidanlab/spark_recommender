package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap

/**
 * This is the results returned by a feature processing unit. 
 * 
 * success:Boolean stores if this feature is successful
 * 
 * resourceMap: stores a (key, resource) pair to be handled by FeatureHandler. 
 * 
 * resourceIden: a unique identifier for this feature (to be used in HashMap purpose).
 * 
 * Usage example: 
 * 1) When the resource has failed due to any reason. 
 * 		return FeatureResource.fail
 *   
 * 2) When features have been built. 
 * 		//get identity
 *   	val idenStr:String = someIdentityFunction(...)
 *   	//set resource
 *   	resourceMap(FeatureResource.ResourceStr_UserFeature) = "hdfs://blah/feature_"+idenStr
 *      resourceMap(FeatureResource.ResourceStr_UserFeatureMap) = "hdfs://blah/featureMap_"+idenStr
 *   	return new FeatureResource(true,resourceMap, idenStr)
 * 
 *  @author jiayu.zhou
 *   
 */
class FeatureResource(var success:Boolean, var resourceMap:Option[HashMap[String, Any]] = Some(new HashMap), var resourceIden:String) {
  
}

object FeatureResource{
	//here we specify some field names for resourceMap
	val ResourceStr_UserFeature = "userFeature"
	val ResourceStr_ItemFeature = "itemFeature"
	val ResourceStr_FeatureDim  = "featureDimension"
	    
	/*
	 * Provides a shortcut with an empty FeatureResource indicating a failed status. 
	 */
	def fail:FeatureResource = {
	   new FeatureResource(false, None, "")
	}
}