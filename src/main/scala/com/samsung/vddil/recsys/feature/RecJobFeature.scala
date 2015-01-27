package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.job.JobWithFeature

/**
 * The recommendation job feature data structure.  
 * 
 * {{{
 * new RecJobUserFeature("Zapping", (freq -> 10)) 
 * new RecJobFactFeature("PMF", (k -> 10, pass -> 1))
 * }}}
 */
sealed trait RecJobFeature{
    /** the feature name used to invoke different feature extraction algorithm */
    def featureName:String
    
    /** feature extraction parameters */
    def featureParams:HashMap[String, String]
    
    /** feature post-processing parameters */
    def postProcessing:List[FeaturePostProcess]
    
    /** Extracts features and store (extracted) feature information in jobStatus */
	def run(jobInfo: JobWithFeature):Unit
}

/** Item feature (program feature) e.g., genre  */
case class RecJobItemFeature(
        featureName:String, 
        featureParams:HashMap[String, String],
        postProcessing:List[FeaturePostProcess]) extends RecJobFeature{
	def run(jobInfo: JobWithFeature) = {
	   jobInfo.jobStatus.completedItemFeatures(this) 
	   	  = ItemFeatureHandler.processFeature(featureName, featureParams, postProcessing, jobInfo)
	}
}

/** User feature e.g., watch time, zapping */
case class RecJobUserFeature(
        featureName:String, 
        featureParams:HashMap[String, String],
        postProcessing:List[FeaturePostProcess]) extends RecJobFeature{
	def run(jobInfo: JobWithFeature) = {
	   jobInfo.jobStatus.completedUserFeatures(this) 
	   	  = UserFeatureHandler.processFeature(featureName, featureParams, postProcessing, jobInfo)
	}
}

/** Factorization-based (collaboration filtering) features. */
case class RecJobFactFeature(
        featureName:String, 
        featureParams:HashMap[String, String],
        postProcessing:List[FeaturePostProcess]) extends RecJobFeature{
	def run(jobInfo: JobWithFeature) = {
	    jobInfo.jobStatus.completedFactFeatures(this) 
	       = FactFeatureHandler.processFeature(featureName, featureParams, postProcessing, jobInfo)
	}
}