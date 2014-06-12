package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap

/*
 * This is the results returned by a feature processing unit. 
 * 
 * success:Boolean stores if this feature is successful
 * 
 * 
 */
class FeatureResource(var success:Boolean, var resourceMap:HashMap[String, Any] = null) {
	
}

object FeatureResource{
	//here we specify some field names
	val ResourceStr_UserFeature = "userFeature"
	val ResourceStr_ItemFeature = "itemFeature"
}