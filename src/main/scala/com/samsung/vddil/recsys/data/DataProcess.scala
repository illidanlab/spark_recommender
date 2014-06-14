package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object DataProcess {
	def prepare(jobInfo:RecJob){
	  
	    val jobStatus:RecJobStatus = jobInfo.jobStatus
	    

	    val sc = new SparkContext("local[16]", "MovieLensALS")
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
	    data.saveAsTextFile(jobInfo.resourceLoc(RecJob.ResourceLoc_JobData)) 
	    
    	//2. generate and maintain user list in JobStatus
	    val users = data.map(_._1).distinct
	    //save users list
	    //TODO: read this from xml
	    users.saveAsTextFile(jobStatus.resourceLocation_UserList) 
	    
    	//3. generate and maintain item list in JobStatus
	    val items = data.map(_._2).distinct
	    //save items list
	    //TODO: read this from xml
	    items.saveAsTextFile(jobStatus.resourceLocation_UserList)
	    
	}
}