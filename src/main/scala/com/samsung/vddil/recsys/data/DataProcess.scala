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
							sc: SparkContext):Option[RDD[(String, String, Double)]] = {
		
		//read all data mentioned in test dates l date
		//get RDD of data of each individua
		val arrRatingsRDD = dates.map{date => 
									  sc.textFile(pathPrefix + date)
										.map {line =>    //convert each line of file to rating
										  	  val fields = line.split('\t')
										  	  (fields(0), fields(1), fields(2).toDouble)
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
            //2. generate and maintain user list in JobStatus
            val users = data.map(_._1).distinct
            users.persist 
            jobStatus.users = users.collect
            
            //3. generate and maintain item list in JobStatus
            val items = data.map(_._2).distinct
            items.persist 
            jobStatus.items = items.collect
            
            //create mapping from string id to int
            val userIdMap =  (jobStatus.users zip (1 to jobStatus.users.length)).toMap
            jobStatus.userIdMap = Some(userIdMap)
            
            val itemIdMap = (jobStatus.items zip (1 to jobStatus.items.length)).toMap
            jobStatus.itemIdMap = Some(itemIdMap)

            //broadcast these map to worker nodes
            val bUMap = sc.broadcast(userIdMap)
            val bIMap = sc.broadcast(itemIdMap)

            //save merged data
            jobStatus.resourceLocation_CombineData = dataLocCombine
            if(jobInfo.outputResource(dataLocCombine)) {
                Logger.info("Dumping combined data")
                data.map{record => bUMap.value(record._1) + "," + bIMap.value(record._2) + "," + record._3}
                    .saveAsTextFile(dataLocCombine) 
            }
            
            
            //save users list
            jobStatus.resourceLocation_UserList    = dataLocUserList
            if(jobInfo.outputResource(dataLocUserList)){
               Logger.info("Dumping user list")
               users.saveAsTextFile(dataLocUserList)
            }       


            //save users map
            //TODO:
            
            
            //save items list
            jobStatus.resourceLocation_ItemList    = dataLocItemList
            if(jobInfo.outputResource(dataLocItemList)){
                Logger.info("Dumping item list")
                items.saveAsTextFile(dataLocItemList)
            } 

            //save items map
            //TODO:
            

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
		
    //get userMap and itemMap
     jobInfo.jobStatus.userIdMap foreach { userMap =>
      jobInfo.jobStatus.itemIdMap foreach { itemMap =>
        //broadcast user and item map
        val bUMap = sc.broadcast(userMap)
        val bIMap = sc.broadcast(itemMap)

        //read all data mentioned in test dates l date
        //get RDD of data of each individua
        val testData = getDataFromDates(jobInfo.testDates, 
                        jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
                        sc)
        
        //include only users and items seen in training
        testData foreach {data =>
          val filtData = data.filter{x => 
                            bUMap.value.contains(x._1) && bIMap.value.contains(x._2)
                          }.map {x =>
                            Rating(bUMap.value(x._1), bIMap.value(x._2), x._3)  
                          }
          jobInfo.jobStatus.testWatchTime = Some(filtData)
          
        }
      }  
    }

  }


	
}
