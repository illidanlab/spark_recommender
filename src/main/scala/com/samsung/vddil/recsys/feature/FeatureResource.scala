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
 *  @author jiayu.zhou
 *   
 */
class FeatureResource(var success:Boolean, var resourceMap:HashMap[String, Any] = new HashMap, var resourceIden:String) {
  
}

object FeatureResource{
	//here we specify some field names for resourceMap
	val ResourceStr_UserFeature = "userFeature"
	val ResourceStr_ItemFeature = "itemFeature"
	
	/*
	 * Provides a shortcut with an empty FeatureResource indicating a failed status. 
	 */
	def fail:FeatureResource = {
	   new FeatureResource(false, null, "")
	}
}