package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * 
 * Modified by Jiayu: added resource path
 */
object DataProcess {
	def prepare(jobInfo:RecJob){
	  
	    val jobStatus:RecJobStatus = jobInfo.jobStatus
	    

	    //get the spark context
	    val sc = jobInfo.sc
	    
	    //1. aggregate the dates and generate sparse matrix in JobStatus
	    //? sparse matrix in what form
	    //read
	    val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + jobInfo.trainDates(0)
	    var data = sc.textFile(fileName).map { line =>
	    		val fields = line.split('\t')
	    		//user, item, watchtime
	    		(fields(0), fields(1), fields(2))
	    }
	    
	    for (trainDate <- jobInfo.trainDates.tail) {
	    	val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + trainDate
	    	val dataNext = sc.textFile(fileName).map { line =>
	    		val fields = line.split('\t')
	    		//user, item, watchtime
	    		(fields(0), fields(1), fields(2))
	    	}
	    	data = data.union(dataNext)
	    }
	    
	    //save merged data
	    jobStatus.resourceLocation_CombineData
	    	= jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/combineData"
	    data.saveAsTextFile(jobStatus.resourceLocation_CombineData) 
	    
    	//2. generate and maintain user list in JobStatus
	    val users = data.map(_._1).distinct
	    users.persist
	    //save users list
	    jobStatus.resourceLocation_UserList 
	    	= jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/userList"
	    users.saveAsTextFile(jobStatus.resourceLocation_UserList) 
	    jobStatus.users = users.collect
	    
    	//3. generate and maintain item list in JobStatus
	    val items = data.map(_._2).distinct
	    items.persist
	    //save items list
	    jobStatus.resourceLocation_ItemList
	    	= jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemList"
	    items.saveAsTextFile(jobStatus.resourceLocation_ItemList)
	    jobStatus.items = items.collect
	    
	    //unpersist the persisted objects
	    users.unpersist(false)
	    items.unpersist(false)
	}
}