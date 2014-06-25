package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.utils.HashString
import org.apache.hadoop.fs.Path
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.Rating

/**
 * 
 * Modified by Jiayu: added resource path
 */
object DataProcess {
    /**
     *   This method generates ( UserID, ItemID, feedback ) tuples from ACR watch time data
     *   and stores the tuples, list of UserID, list of ItemID into the system. 
     */
	def prepareTrain(jobInfo:RecJob) {
	    
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
	  
	    val dataLocCombine  = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/combineData_" + dataHashingStr
	    val dataLocUserList = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/userList_" + dataHashingStr
	    val dataLocItemList = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemList_" + dataHashingStr
	    
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
	    
	    for (trainDate <- jobInfo.trainDates.tail) yield {
	    	val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + trainDate
	    	val dataNext = sc.textFile(fileName).map { line =>
	    		val fields = line.split('\t')
	    		//user, item, watchtime
	    		(fields(0), fields(1), fields(2))
	    	}
	    	data = data.union(dataNext)
	    }
	    
	    //save merged data
	    jobStatus.resourceLocation_CombineData = dataLocCombine
	    if(jobInfo.outputResource(dataLocCombine)) {
	    	Logger.info("Dumping combined data")
		    data.map{s => s._1 + "," + s._2 + "," + s._3}
		        .saveAsTextFile(dataLocCombine) 
	    }
	    
    	//2. generate and maintain user list in JobStatus
	    val users = data.map(_._1).distinct
	    users.persist
	    //save users list
	    jobStatus.resourceLocation_UserList    = dataLocUserList
	    if(jobInfo.outputResource(dataLocUserList)){
	       Logger.info("Dumping user list")
	       users.saveAsTextFile(dataLocUserList)
	    }  
	    jobStatus.users = users.collect
	    
    	//3. generate and maintain item list in JobStatus
	    val items = data.map(_._2).distinct
	    items.persist
	    //save items list
	    jobStatus.resourceLocation_ItemList    = dataLocItemList
	    if(jobInfo.outputResource(dataLocItemList)){
	    	Logger.info("Dumping item list")
	    	items.saveAsTextFile(dataLocItemList)
	    } 
	    jobStatus.items = items.collect
	    
	    //unpersist the persisted objects
	    users.unpersist(false)
	    items.unpersist(false)
	}
	
	
	/*
	 * read the data from test dates into RDD form
	 */
	def prepareTest(jobInfo: RecJob)  = {
		//get spark context
		val sc  = jobInfo.sc
		
		//read all data mentioned in test dates 
		val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + 
		                    jobInfo.testDates.head
		var data = sc.textFile(fileName).map { line =>
		    val fields = line.split('\t')
		    Rating(fields(0), fields(1), fields(2).toDouble)
		}
		
		for (testDate <- jobInfo.testDates.tail) {
			val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime) + testDate
			data = data.union(sc.textFile(fileName).map { line =>
                                val fields = line.split('\t')
                                Rating(fields(0), fields(1), fields(2).toDouble)
			                  })
		}
		
		data.persist
		jobInfo.jobStatus.testWatchTime = Some(data)
	}
	
	
}