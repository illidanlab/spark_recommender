package com.samsung.vddil.recsys.job

/*
 * Constants for XML tags. 
 */
object JobTag {
  
	val JOB_LIST = "jobList"
	val JOB_ENTRY = "jobEntry"
	  
	val JOB_TYPE = "jobType"
	val JOB_NAME = "jobName"
	val JOB_DESC = "jobDesc"
  
	val JOB_TYPE_RECOMMENDATION = "recommendation"
	val JOB_TYPE_HELLOWOLRD = "helloworld"
	  
	  
	val RECJOB_TRAIN_DATE_LIST = "trainDates"
	val RECJOB_TRAIN_DATE_UNIT = "date"
	  
	val RECJOB_FEATURE_LIST = "features"
	val RECJOB_FEATURE_TYPE_ITEM = "itemFeature"
	val RECJOB_FEATURE_TYPE_USER = "userFeature"
	val RECJOB_FEATURE_TYPE_FACT = "factFeature"
	val RECJOB_FEATURE_UNIT = "feature"
	val RECJOB_FEATURE_UNIT_NAME = "name"
	val RECJOB_FEATURE_UNIT_TYPE = "type"
	val RECJOB_FEATURE_UNIT_PARAM = "param"
	
	val RECJOB_MODEL_LIST = "models"
	val RECJOB_MODEL_TYPE_SCRREG = "score_reg"
	val RECJOB_MODEL_TYPE_BINCLS = "binary_cls" 
	val RECJOB_MODEL_UNIT = "model"
	val RECJOB_MODEL_UNIT_NAME = "name"
	val RECJOB_MODEL_UNIT_TYPE = "type"
	val RECJOB_MODEL_UNIT_PARAM = "param"
}