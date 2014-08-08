package com.samsung.vddil.recsys.job

/**
 * Constants for XML tags. 
 * 
 * These tags are used to parse XML file into Scala data structures. 
 * 
 */
object JobTag {
  
	val JobList = "jobList"
	val JobEntry = "jobEntry"
	  
	val JobType = "jobType"
	val JobName = "jobName"
	val JobDesc = "jobDesc"
  
	val JobType_Recommendation = "recommendation"
	val JobType_HelloWorld = "helloworld"
	
	  
	val RecJobSparkContext = "sparkContext"
    val RecJobSparkContextMaster = "master"
    val RecJobSparkContextJobName = "jobName" // by default use the job name of RecJob. 
	
    val RecJobTrainDateList = "trainDates"
	val RecJobTrainDateUnit = "date"
	  
	val RecJobTestDateList = "testDates"
	val RecJobTestDateUnit = "date"
	val RecJobTestList = "tests"
	val RecJobTestUnit = "test"
	val RecJobTestUnitName = "name"
	val RecJobTestUnitType = "type"
	val RecJobTestUnitParam = "param"
	val RecJobTestType_NotCold = "futureNotColdstart"
	val RecJobTestType_ColdItems = "futureColdItems"
	val RecJobTestType_ColdUsers = "futureColdUsers"
	val RecJobTestType_ColdItemUsers = "futureColdItemUsers"
		
	val RecJobMetricList = "metrics"
	val RecJobMetricUnit = "metric"
	val RecJobMetricUnitName = "name"
    val RecJobMetricUnitType = "type"
    val RecJobMetricUnitParam = "param"
	val RecJobMetricType_MSE = "mse"
	val RecJobMetricType_RMSE = "rmse"
	val RecJobMetricType_HR = "hr"
	val RecJobMetricType_ColdRecall = "cold_recall"

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
