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
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.utils.Logger

/**
 * Provides functions to aggregate data. The main functions are [[DataProcess.prepareTrain]] 
 * and [[DataProcess.prepareTest]], which provides training and testing.  
 */
object DataProcess {


    /**
     * Replaces String-type ID to Integer. 
     * 
     * Replaces String-type ID in the RDD[(String, (Int, Double))]
     * with a given String-Integer and returns RDD[(Int, Int, Double)]
     * 
     * @param idMap the String-Integer map
     * @param dataRDD the data RDD of the form (String, (Int, Double)) 
     * @param sc the SparkContext
     */
    def substituteIntId(idMap:Map[String, Int], 
                      dataRDD:RDD[(String, (Int,Double))],
                      sc:SparkContext):RDD[(Int, Int, Double)] = {
        sc.parallelize(idMap.toList).join(dataRDD).map{x => //(StringId,(intId,(Int, Double)))
            val intID:Int = x._2._1 
            val otherField = x._2._2._1
            val value:Double = x._2._2._2
            (intID, otherField, value) 
        }
    }


    /**
     * Returns the combined data, given the physical location of data repository and a set of dates.
     * 
     * For each date, the function access the data as 
     * 
     * {{{
     * val oneDay:String = dates(0)
     * val fileLoc:String = "pathPrefix/oneDay/"
     * }}}
     * 
     * @param dates a list of dates, from which the data is required. 
     * @param pathPrefix the physical location of the data repository 
     * 
     * @return the combined data
     */
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
     * Generates user-item matrix, user list, and item list. 
     * 
     * This method generates ( UserID, ItemID, feedback ) tuples from ACR watch time data
     * and stores the tuples, list of UserID, list of ItemID into the system. 
     * 
     * The following data fields will be filled:
     * `jobStatus.users`,
     * `jobStatus.items`,
     * `jobStatus.userIdMap`,
     * `jobStatus.itemIdMap`,
     * `jobStatus.resourceLocation_CombineData`, 
     * `jobStatus.resourceLocation_UserList`,
     * `jobStatus.resourceLocation_ItemList`.
     * 
     *  @param jobInfo the job information
     *   
     */
	def prepareTrain(jobInfo:RecJob) {
	    
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
	  
	    val dataLocCombine  = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/combineData_" + dataHashingStr
	    val dataLocUserList = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/userList_" + dataHashingStr
	    val dataLocItemList = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemList_" + dataHashingStr
	    val dataLocUserMap = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/userMap_" + dataHashingStr
        val dataLocItemMap = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/itemMap_" + dataHashingStr

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
            jobStatus.userIdMap = userIdMap

            val itemIdMap = (jobStatus.items zip (1 to jobStatus.items.length)).toMap
            jobStatus.itemIdMap = itemIdMap

            //broadcast only item map as its small to worker nodes
            val bIMap = sc.broadcast(itemIdMap)

            //save merged data
            jobStatus.resourceLocation_CombineData = dataLocCombine
            if(jobInfo.outputResource(dataLocCombine)) {
                Logger.info("Dumping combined data")
                val replacedItemIds =  data.map{record => 
                  (record._1, (bIMap.value(record._2), record._3))
                }
                val replacesUserIds = substituteIntId(userIdMap,
                                        replacedItemIds, sc).map {x =>
                                          x._1 + "," + x._2 + "," + x._3
                                        }
                replacesUserIds.saveAsTextFile(dataLocCombine) 
            }
            
            //save users list
            jobStatus.resourceLocation_UserList    = dataLocUserList
            if(jobInfo.outputResource(dataLocUserList)){
               Logger.info("Dumping user list")
               users.saveAsTextFile(dataLocUserList)
            }

            //save user map
            if (jobInfo.outputResource(dataLocUserMap)) {
              Logger.info("Dumping user map")
              sc.parallelize(userIdMap.toList).saveAsTextFile(dataLocUserMap)
            }

            //save items list
            jobStatus.resourceLocation_ItemList    = dataLocItemList
            if(jobInfo.outputResource(dataLocItemList)){
                Logger.info("Dumping item list")
                items.saveAsTextFile(dataLocItemList)
            } 

            //save items map
            if (jobInfo.outputResource(dataLocItemMap)) {
              Logger.info("Dumping item map")
              sc.parallelize(itemIdMap.toList).saveAsTextFile(dataLocItemMap)
            }

            //unpersist the persisted objects
            users.unpersist(false)
            items.unpersist(false)
		}
	}
	
	
	/**
	 * Reads the data from test dates into RDD form.
	 * 
	 * The following resource will be filled:
	 * `jobInfo.jobStatus.testWatchTime`.
	 * 
	 * @param jobInfo the job information
	 */
	def prepareTest(jobInfo: RecJob)  = {

	    //get spark context
		  val sc  = jobInfo.sc
	
	    //get userMap and itemMap
	    val userMap = jobInfo.jobStatus.userIdMap 
	    val itemMap = jobInfo.jobStatus.itemIdMap   
	    
	    //broadcast item map
	    val bIMap = sc.broadcast(itemMap)
	
	    //read all data mentioned in test dates l date
	    //get RDD of data of each individual
	    val testData = getDataFromDates(jobInfo.testDates, 
	                    jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
	                    sc)
	
	    //include only users and items seen in training
	    testData foreach {data =>
	      val replacedItemIds =  data.filter(x => bIMap.value.contains(x._2)
	                                  ).map{record =>
	                      (record._1, (bIMap.value(record._2), record._3))
	                    }
	      val replacedUserIds = substituteIntId(userMap,
	                                            replacedItemIds, sc)    
	      jobInfo.jobStatus.testWatchTime = Some(replacedUserIds.map{x => 
	                                            Rating(x._1, x._2, x._3)
	                                        })
	    }
    }

}
