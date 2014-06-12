package com.samsung.vddil.recsys.job

/*
 * Constants for XML tags. 
 */
object JobTag {
  
	val JobList = "jobList"
	val JobEntry = "jobEntry"
	  
	val JobType = "jobType"
	val JobName = "jobName"
	val JobDesc = "jobDesc"
  
	val JobType_Recommendation = "recommendation"
	val JobType_HelloWorld = "helloworld"
	  
    val RecJobTrainDateList = "trainDates"
	val RecJobTrainDateUnit = "date"
	  
	val RecJobFeatureList = "features"
	val RecJobFeatureType_Item = "itemFeature"
	val RecJobFeatureType_User = "userFeature"
	val RecJobFeatureType_Fact = "factFeature"
	  
	val RecJobFeatureUnit = "feature"
	val RecJobFeatureUnitName = "name"
	val RecJobFeatureUnitType = "type"
	val RecJobFeatureUnitParam = "param"
	
	val RecJobModelList = "models"
	val RecJobModelType_Regress = "score_reg"
	val RecJobModelType_Classify = "binary_cls" 
	val RecJobModelUnit = "model"
	val RecJobModelUnitName = "name"
	val RecJobModelUnitType = "type"
	val RecJobModelUnitParam = "param"
}