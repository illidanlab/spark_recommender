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
  
	def getDataFromDates(dates:Array[String], 
							pathPrefix: String,
							sc: SparkContext):Option[RDD[Rating]] = {
		
		//read all data mentioned in test dates l date
		//get RDD of data of each individua
		val arrRatingsRDD = dates.map{date => 
									  sc.textFile(pathPrefix + date)
										.map {line =>    //convert each line of file to rating
										  	  val fields = line.split('\t')
										  	  Rating(fields(0), fields(1), fields(2).toDouble)
										   }
								  }
		
		//combine RDDs to get the full data
		arrRatingsRDD.length match {
			case 0 => None
			case _ => Some(arrRatingsRDD.reduce((a,b) => a.union(b)))
		}
	}
  
  
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
	    //read
	    val trainData = getDataFromDates(jobInfo.trainDates, 
	    							jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
	    							sc)
	    
    	trainData foreach { data =>
			//save merged data
            jobStatus.resourceLocation_CombineData = dataLocCombine
            if(jobInfo.outputResource(dataLocCombine)) {
                Logger.info("Dumping combined data")
                data.map{record => record.user + "," + record.item + "," + record.rating}
                    .saveAsTextFile(dataLocCombine) 
            }
            
            //2. generate and maintain user list in JobStatus
            val users = data.map(_.user).distinct
            users.persist
            //save users list
            jobStatus.resourceLocation_UserList    = dataLocUserList
            if(jobInfo.outputResource(dataLocUserList)){
               Logger.info("Dumping user list")
               users.saveAsTextFile(dataLocUserList)
            }  
            jobStatus.users = users.collect
            
            //3. generate and maintain item list in JobStatus
            val items = data.map(_.item).distinct
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
	    
	}
	
	
	/*
	 * read the data from test dates into RDD form
	 */
	def prepareTest(jobInfo: RecJob)  = {
		//get spark context
		val sc  = jobInfo.sc
		
		//read all data mentioned in test dates l date
		//get RDD of data of each individua
		val data = getDataFromDates(jobInfo.testDates, 
	    							jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
	    							sc)
				
		jobInfo.jobStatus.testWatchTime = data
		
		jobInfo.jobStatus.testWatchTime foreach {data =>
		    data.persist
		} 
	}
	
	
}