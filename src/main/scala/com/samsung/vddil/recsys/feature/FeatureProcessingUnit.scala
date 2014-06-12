package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob

/*
 * This is a trait for feature processing unit. 
 * 
 * Each feature processing unit is an object that 
 * provides a static method processFeature that takes 
 * three 
 */
trait FeatureProcessingUnit {
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob)
}