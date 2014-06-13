package com.samsung.vddil.recsys.job

/**
 * Constants for XML tags. 
 * 
 * These tags are used to parse XML file into Scala data structures. 
 * 
 *  @author jiayu.zhou 
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
	  
	val RecJobResourceLocation = "resourceLocation"
	val RecJobResourceLocationRoviHQ = "roviHq"
	val RecJobResourceLocationWatchTime = "watchTime"
	val RecJobResourceLocationWorkspace = "workspace"
	  
	val RecJobDataSplit = "dataSplit"
	val RecJobDataSplitTestRatio = "testingRatio"
	val RecJobDataSplitValidRatio = "validationRatio"
	  
	val RecJobEnsembleList = "ensembles"
	val RecJobEnsembleUnit = "ensemble"
	val RecJobEnsembleUnitType  = "type"
	val RecJobEnsembleUnitParam = "param"
}