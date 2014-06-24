package com.samsung.vddil.recsys

object CfgTag {
	val CfgList = "cfgList"
	  
	val SparkContext_master_default = "local[2]"
	  
	val CfgSparkContext = "sparkContext"
    val CfgSparkContextMaster = "master"
    val CfgSparkContextJobName = "jobName" // by default use the job name of RecJob. 
}