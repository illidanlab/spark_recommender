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
import org.apache.spark.storage.StorageLevel

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
                      sc:SparkContext,
                      partitionNum:Int
                ):RDD[(Int, Int, Double)] = {
        val idMapList = idMap.toList

        val idMapRDD = sc.parallelize(idMap.toList)
        Logger.info("Parallelized idMap.toList")

        idMapRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //dataRDD .repartition(partitionNum)

        val result = idMapRDD.join(dataRDD).map{x => //(StringId,(intId,(Int, Double)))
            val intID:Int = x._2._1 
            val otherField = x._2._2._1
            val value:Double = x._2._2._2
            (intID, otherField, value) 
        }
        Logger.info("substituteIntId completes")
        result
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
							sc: SparkContext, 
							unitParitionNum: Int
						):Option[RDD[(String, String, Double)]] = {
		//read all data mentioned in test dates l date
		//get RDD of data of each individua
		val arrRatingsRDD = dates.map{date => 
									  sc.textFile(pathPrefix + date)
										.map {line =>    //convert each line of file to rating
										  	  val fields = line.split('\t')
										  	  (fields(0), fields(1), fields(2).toDouble)
								      }.repartition(unitParitionNum)
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
    	val dataDates = jobInfo.trainDates
	    val watchTimeResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime)
	    val sc = jobInfo.sc
	    val outputResource = (x:String) => jobInfo.outputResource(x)
	    val outputDataResLoc = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData)
	    val partitionNum = jobInfo.partitionNum_train 
	        
	    val combData:Option[CombinedDataSet] = combineWatchTimeData(
	        dataDates:Array[String], 
	        watchTimeResLoc:String, 
	        sc:SparkContext, 
	        partitionNum:Int,
	        outputResource: String=>Boolean,
	        outputDataResLoc: String ) 
	        
	    if (combData.isDefined){
	        val comDataStruct = combData.get 
		    val jobStatus:RecJobStatus = jobInfo.jobStatus
		    
		    jobStatus.resourceLocation_CombinedData_train = combData
	    }else{
	        Logger.error("Failed to combine training data!")
	    }
	}
	
	/**
	 * Generates combined watch time data set. 
	 * 
	 * @param dataDates
	 * @param watchTimeResLoc
	 * @param sc the instance of Spark Context. 
	 * @param outputResource whether the resource should be output. 
	 * @param outputDataResLoc the directory in which data resources are output.
	 */
	def combineWatchTimeData(
	        dataDates:Array[String], 
	        watchTimeResLoc:String, 
	        sc:SparkContext, 
	        partitionNum:Int,
	        outputResource: String=>Boolean,
	        outputDataResLoc: String ): Option[CombinedDataSet] = {
	  
        val resourceStr = CombinedDataSet.resourcePrefix + HashString.generateOrderedArrayHash(dataDates) 
        
	    val dataLocCombine  = outputDataResLoc + "/" + resourceStr
	    val dataLocUserList = dataLocCombine + "_userList"
	    val dataLocItemList = dataLocCombine + "_itemList"
	    val dataLocUserMap  = dataLocCombine + "_userMap"
        val dataLocItemMap  = dataLocCombine + "_itemMap"

	    //1. aggregate the dates and generate sparse matrix in JobStatus
	    //read
	    val trainData = getDataFromDates(dataDates, watchTimeResLoc, sc, partitionNum)
	    
    	if(trainData.isDefined){ 
    	    val data = trainData.get.persist(StorageLevel.DISK_ONLY)
    	    
            //2. generate and maintain user list in JobStatus
            val users = data.map(_._1).distinct
            val userList = users.collect
            Logger.info("User number: " + userList.size) 
            users.persist 

            //3. generate and maintain item list in JobStatus
            val items = data.map(_._2).distinct
            val itemList = items.collect
            Logger.info("Item number: " + itemList.size)
            items.persist 
            
            //create mapping from string id to int
            val userIdMap = (userList zip (1 to userList.length)).toMap
            val itemIdMap = (itemList zip (1 to itemList.length)).toMap

            //broadcast only item map as its small to worker nodes
            val bIMap = sc.broadcast(itemIdMap)

            //save merged data
            if(outputResource(dataLocCombine)) {
                Logger.info("Dumping combined data")
                val replacedItemIds =  data.map{record => 
                	  (record._1, (bIMap.value(record._2), record._3))
                      }.persist(StorageLevel.DISK_ONLY)

                val replacesUserIds = substituteIntId(userIdMap, 
                      replacedItemIds, sc, partitionNum).map {x =>
                           x._1 + "," + x._2 + "," + x._3
                      }.persist(StorageLevel.DISK_ONLY)

                replacesUserIds.saveAsTextFile(dataLocCombine)
		
                replacedItemIds.unpersist()
                replacesUserIds.unpersist() 
                data.unpersist()
            }
            
            //save users list
            if(outputResource(dataLocUserList)){
                Logger.info("Dumping user list")
                users.saveAsTextFile(dataLocUserList)
            }

            //save user map
            if (outputResource(dataLocUserMap)) {
                Logger.info("Dumping user map")
                sc.parallelize(userIdMap.toList).saveAsTextFile(dataLocUserMap)
            }

            //save items list
            if(outputResource(dataLocItemList)){
                Logger.info("Dumping item list")
                items.saveAsTextFile(dataLocItemList)
            } 

            //save items map
            if (outputResource(dataLocItemMap)) {
                Logger.info("Dumping item map")
                sc.parallelize(itemIdMap.toList).saveAsTextFile(dataLocItemMap)
            }

            //unpersist the persisted objects
            users.unpersist(false)
            items.unpersist(false)
            
            val userListObj = CombinedDataEntityList  (userList,  dataLocUserList)
            val itemListObj = CombinedDataEntityList  (itemList,  dataLocItemList)
            val userMapObj  = CombinedDataEntityIdMap (userIdMap, dataLocUserMap)
            val itemMapObj  = CombinedDataEntityIdMap (itemIdMap, dataLocItemMap)
            
            //create a data structure maintaining all resources. 
            Some(new CombinedDataSet(
                resourceStr, dataLocCombine,
                userListObj, itemListObj, userMapObj, itemMapObj,
                dataDates))
		}else{
			None
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
    
    val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.testDates)  
    val dataLocTest = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/testData_" + dataHashingStr
	
    val trainCombData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
    
    //get spark context
	val sc  = jobInfo.sc
	val partitionNum = jobInfo.partitionNum_test

    //get userMap and itemMap
    val userMap = trainCombData.userMap.mapObj
    val itemMap = trainCombData.itemMap.mapObj   
    
    //broadcast item map
    val bIMap = sc.broadcast(itemMap)

    //read all data mentioned in test dates l date
    //get RDD of data of each individua
    val testData = getDataFromDates(jobInfo.testDates, 
                    jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
                    sc, partitionNum)

    //include only users and items seen in training
    testData foreach {data =>
      val replacedItemIds =  data.persist(StorageLevel.DISK_ONLY).
                    filter(x => bIMap.value.contains(x._2)).
                    map{record =>
                      (record._1, (bIMap.value(record._2), record._3))
                    }.persist(StorageLevel.DISK_ONLY)

      val replacedUserIds = substituteIntId(userMap, replacedItemIds, sc, partitionNum).persist(StorageLevel.DISK_ONLY)
    
      jobInfo.jobStatus.testWatchTime = Some(replacedUserIds.map{x => 
                                            Rating(x._1, x._2, x._3)
                                        })
                                        
      jobInfo.jobStatus.testWatchTime foreach {testData=>
        if (jobInfo.outputResource(dataLocTest)) {
          testData.saveAsObjectFile(dataLocTest)
        }
      }
      
      jobInfo.jobStatus.testWatchTime = Some(sc.objectFile[Rating](dataLocTest))
    }

  }

}
