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
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import scala.collection.mutable.HashMap

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

	    Logger.info("Starting to repartition and persist")
        
        idMapRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //dataRDD .repartition(3200)
        //Consider HashPartitioner.

	    val pData = dataRDD.partitionBy(Pipeline.getHashPartitioner(partitionNum))
                           .persist(StorageLevel.DISK_ONLY)	

        Logger.info("Repartition done with paritions" + partitionNum)

	
        //var result = dataRDD.partitionBy(Pipeline.getHashPartitioner(3200)).
        var result = pData.
                 join(idMapRDD, partitionNum).map{x => //(StringId, ((Int, Double), intd))
            val intID:Int    = x._2._2
            val otherField   = x._2._1._1
            val value:Double = x._2._1._2
            (intID, otherField, value)
        }

        //val result = idMapRDD.join(dataRDD).map{x => //(StringId,(intId,(Int, Double)))
        //    val intID:Int = x._2._1 
        //    val otherField = x._2._2._1
        //    val value:Double = x._2._2._2
        //    (intID, otherField, value) 
        //}
        Logger.info("substituteIntId completes")
        result
    }


    def substituteIntId(
            dataRDD:RDD[(String, String, Double)],
            userIdMapRDD:RDD[(String, Int)],
            itemIdMapRDD:RDD[(String, Int)],
            partitionNum:Int
        ): RDD[(Int, Int, Double)] = {
    	
        val pData = dataRDD.map{line =>
            val userIdStr:String = line._1
            val itemIdStr:String = line._2
            val rating:Double = line._3
            (userIdStr, (itemIdStr, rating))
        }//.partitionBy(Pipeline.getHashPartitioner(1000))
        
        pData.join(userIdMapRDD, partitionNum).map{ line => //(userIdStr, ((itemIdStr, rating), userIdInt))
            val userIdInt:Int    = line._2._2
            val itemIdStr:String = line._2._1._1
            val rating:Double    = line._2._1._2
            (itemIdStr, (userIdInt, rating))
        }.join(itemIdMapRDD, partitionNum).map{ line => //(itemIdStr, ((userIdInt, rating), itemIdInt)) 
            val itemIdInt:Int    = line._2._2
            val userIdInt:Int    = line._2._1._1
            val rating:Double    = line._2._1._2
            (userIdInt, itemIdInt, rating)
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
								      }//.repartition(unitParitionNum)
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
	        outputDataResLoc: String,
	        jobInfo.dataProcessParam) 
	     
	    if (combData.isDefined){
	        val comDataStruct = combData.get 
		    val jobStatus:RecJobStatus = jobInfo.jobStatus
		    jobStatus.resourceLocation_CombinedData_train = combData
	    }else{
	        Logger.error("Failed to combine training data!")
	    }
	}
	
	
	/**
     * Generates user-item matrix, user list, and item list. 
     * 
     * This method generates ( UserID, ItemID, feedback ) tuples from ACR watch time data
     * and stores the tuples, list of UserID, list of ItemID into the system. 
     * 
     *  @param jobInfo the job information
     *   
     */
	def prepareTrain(jobInfo:RecMatrixFactJob) {
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
	        outputDataResLoc: String, 
	        jobInfo.dataProcessParam)
	        //DONE: extract data normalization. Same as for RecJob 
	        
	    if (combData.isDefined){
	        val comDataStruct = combData.get 
		    jobInfo.jobStatus.resourceLocation_CombinedData_train = combData
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
	        outputDataResLoc: String,
	        param:HashMap[String, String]): Option[CombinedDataSet] = {
	  
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
    	    var data = trainData.get.persist(StorageLevel.DISK_ONLY)
    	    		
    	    //add transformation.
    	    val paramList = param.toList
    	    if (paramList.isEmpty) {
    	        data = DataTransformation.transformation_None(data)
    	    }
    	    else {
    	        /**
    	        for (tmp <- paramList) {
    	            tmp._1 match{
    	                case DataTransformation.dataTransformation_Max => 
    	                     data = DataTransformation.transformation_Max(data, sc)
    	                case DataTransformation.dataTransformation_totalWatchTime => 
    	                     data = DataTransformation.transformation_Total(data, sc)    
    	                case _ => data = DataTransformation.transformation_None(data)
    	            }
    	        }
    	        */
    	    	val tmp = paramList(0)
	            tmp._1 match{
	                case DataTransformation.dataTransformation_totalWatchTime => 
	                     data = DataTransformation.transformation_Total(data, sc)    
	                case DataTransformation.dataTransformation_Max => 
	                     data = DataTransformation.transformation_Max(data, sc)     
	                case _ => data = DataTransformation.transformation_None(data)
	            }
    	    }
    	        	    
    	    
    	    
            //2. generate and maintain user list in JobStatus
    	    if (outputResource(dataLocUserList)){
    	        Logger.info("Dumping user list")
    	    	data.map(_._1).distinct.saveAsTextFile(dataLocUserList)
    	    }
    	    val userListRDD  = sc.textFile(dataLocUserList)
            val userSize:Int = userListRDD.count.toInt
            Logger.info("User number: " + userSize) 

            //3. generate and maintain item list in JobStatus
            if (outputResource(dataLocItemList)){
                Logger.info("Dumping item list")
            	data.map(_._2).distinct.saveAsTextFile(dataLocItemList)
            }
    	    val itemListRDD  = sc.textFile(dataLocItemList)
            val itemSize:Int = itemListRDD.count.toInt
            Logger.info("Item number: " + itemSize)
            
            //4. create mappings from string id to integer id
            if (outputResource(dataLocUserMap)){
                Logger.info("Dumping user map")
                val zippedUserIdMap: RDD[(String, Int)] = 
                    userListRDD.zipWithIndex.map{line => (line._1, line._2.toInt)}
                zippedUserIdMap.saveAsObjectFile(dataLocUserMap)
            }
            if (outputResource(dataLocItemMap)){
                Logger.info("Dumping item map")
                val zippedItemIdMap: RDD[(String, Int)] = 
                    itemListRDD.zipWithIndex.map{line => (line._1, line._2.toInt)}
                zippedItemIdMap.saveAsObjectFile(dataLocItemMap)
            }
    	    
    	    //5. substitute Id. 
    	    if (outputResource(dataLocCombine)){
    	        val userIdMapRDD = sc.objectFile[(String, Int)](dataLocUserMap)
    	        val itemIdMapRDD = sc.objectFile[(String, Int)](dataLocItemMap)
    	        
    	        Logger.info("Dumping integer id combined data.")
    	        substituteIntId(data,userIdMapRDD,itemIdMapRDD,partitionNum).
    	        	map(x => x._1 + "," + x._2 + "," + x._3).
    	        	saveAsTextFile(dataLocCombine)
    	    }
    	    //val recordNum:Long = sc.objectFile[(Int, Int, Double)](dataLocCombine).count
    	    val recordNum:Long = sc.textFile(dataLocCombine).count
            
            //create a data structure maintaining all resources. 
            Some(new CombinedDataSet(
                resourceStr, dataLocCombine,
                dataLocUserList, dataLocItemList, dataLocUserMap, dataLocItemMap,
                userSize, itemSize, recordNum, dataDates))
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
	    val userMap = trainCombData.getUserMap()
	    val itemMap = trainCombData.getItemMap()   
	    
	    //broadcast item map
	    val bIMap = sc.broadcast(itemMap)
	
	    //read all data mentioned in test dates l date
	    //get RDD of data of each individua
	    val testData = getDataFromDates(jobInfo.testDates, 
	                    jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
	                    sc, partitionNum)
	
	    if (testData.isDefined){
	        val data = testData.get
	        
	        //include only users and items seen in training
	        if (jobInfo.outputResource(dataLocTest)) {
	        	substituteIntId(data, userMap, itemMap, partitionNum).
	    	        	map(x => Rating(x._1, x._2, x._3)).
	    	        	saveAsObjectFile(dataLocTest)
	        }
	        //TODO: change to CombinedDataSet instance for storage caching. 
	        jobInfo.jobStatus.testWatchTime = Some(sc.objectFile[Rating](dataLocTest).persist(StorageLevel.DISK_ONLY))
	    }

    }
	
    def prepareTest(jobInfo: RecMatrixFactJob) = {
        val sc  = jobInfo.sc
        val partitionNum = jobInfo.partitionNum_test
        
        val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.testDates)  
        val dataLocTest = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/testData_" + dataHashingStr
        
        val trainCombData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
        
        //get userMap and itemMap
        val userMap = trainCombData.getUserMap()
        val itemMap = trainCombData.getItemMap()
        
        //broadcast item map
        val bIMap = sc.broadcast(itemMap)

	    //read all data mentioned in test dates l date
	    //get RDD of data of each individua
	    val testData = getDataFromDates(jobInfo.testDates, 
	                    jobInfo.resourceLoc(RecJob.ResourceLoc_WatchTime), 
	                    sc, partitionNum)
	                    
	    if(testData.isDefined){
	        
	        //generate data RDD. 
	        val data = substituteIntId(testData.get, userMap, itemMap, partitionNum)
	        
	        //create CombinedRawDataSet instance (to be used later). 
	        val dataSet = //CombinedRawDataSet.createInstance(dataHashingStr, dataLocTest, data, jobInfo.testDates)
	            CombinedDataSet.createInstance(dataHashingStr, dataLocTest, 
	                    trainCombData.userListLoc, 
	                    trainCombData.itemListLoc, 
	                    trainCombData.userMapLoc, 
	                    trainCombData.itemMapLoc, 
	                    trainCombData.userNum, 
	                    trainCombData.itemNum, 
	                    trainCombData.dates, data, 
	                    jobInfo.outputResource)
	        
	        //set resource pointer.
	        jobInfo.jobStatus.resourceLocation_CombinedData_test = Some(dataSet)
	    }
    }

}
