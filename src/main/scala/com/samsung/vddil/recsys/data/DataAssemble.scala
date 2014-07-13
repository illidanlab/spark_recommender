package com.samsung.vddil.recsys.data


import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJobStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.feature.FeatureStruct

import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner



/**
 * This version is no longer used and no longer maintained. 
 * Use com.samsung.vddil.recsys.data.DataAssembleObj. 
 * 
 */                            
object DataAssemble {
  
  /**
   * return join of features of specified Ids and ordering of features
   */
  def getCombinedFeatures(idSet: RDD[String], usedFeatures: HashSet[String], 
                              featureResourceMap: HashMap[String, FeatureStruct], 
                              sc: SparkContext): (RDD[(String, String)], List[String]) = {
    
    val usedFeaturesList = usedFeatures.toList
    
    val idSetRDD = idSet.map(x => (x,1))
    
    
    //join all features RDD
    //add first feature to join
    var featureJoin = sc.textFile(featureResourceMap(usedFeaturesList.head).featureFileName)
                        .map { line=> 
                          //first occurence of separator 
                          val sepInd = line.indexOf(',')
                          val id = line.substring(0, sepInd)
                          val features = line.substring(sepInd+1)
                          (id, features)
                        }.join(idSetRDD).map{x =>
                          //id, features , ignore the 1s
                          (x._1, x._2._1)
                        }
       
    //add remaining features
    for (usedFeature <- usedFeaturesList.tail) {
      featureJoin  = featureJoin.join( sc.textFile(featureResourceMap(usedFeature).featureFileName)
                                          .map { line =>
                                                  val sepInd = line.indexOf(',')
                                                  val id = line.substring(0, sepInd)
                                                  val features = line.substring(sepInd+1)
                                                  (id, features)
                                          }
                                        ).map {x =>
                                        (x._1, x._2._1 + "," + x._2._2)
                                      }
    }
        (featureJoin, usedFeaturesList)
  }
  
  
  
  /**
   * will return intersection of ids for which fetures exist
   * usedFeatures: features for which we want intersection of ids
   * featureResourceMap: contains mapping of features to actual files
   * sc: SparkContext
   */
  def getIntersectIds(usedFeatures: HashSet[String], 
            featureResourceMap: HashMap[String, FeatureStruct], 
            sc: SparkContext):  RDD[String] = {
      
      val intersectIds = usedFeatures.map{feature =>
        sc.textFile(featureResourceMap(feature).featureFileName)
          .map(_.split(',')(0)) //assuming first field is always id
      }.reduce((idSetA, idSetB) => idSetA.intersection(idSetB)) // reduce to get instersection of all sets
          
      intersectIds
  }
  
  
  /**
   * will return only those features which satisfy minimum coverage criteria
   * featureResourceMap: contains map of features and location
   * minCoverage: minimum coverage i.e. no. of features found should be greater than this pc
   * sc: spark context
   * total: number of items or users
   */
  def filterFeatures(featureResourceMap: HashMap[String, FeatureStruct], 
      minCoverage: Double, sc: SparkContext, total: Int) :HashSet[String] = {
    //set to keep keys of item feature having desired coverage
        var usedFeatures:HashSet[String] = new HashSet()
        
        //check each feature against minCoverage
      featureResourceMap foreach {
            case (k, v) =>
                {
                    val numFeatures = sc.textFile(v.featureFileName).count
                    if ( (numFeatures.toDouble/total)  > minCoverage) {
                      //coverage satisfy by feature add it to used set
                        usedFeatures += k
                    }
                    
                }
      }
        usedFeatures
  }
  
  
  /*
   *  Joining features 
   */
  def assembleContinuousData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double ):String = {
    
    
      //1. inspect all available features
      //   drop features have low coverage (which significant reduces our training due to missing)
      //   TODO: minUserFeatureCoverage and minItemFeatureCoverage from file. 
    
    
      //get spark context
      val sc = jobInfo.sc
      
      //get num of users
      val numUsers = jobInfo.jobStatus.users.length
      
      //get num of items
      val numItems = jobInfo.jobStatus.items.length
      
      
      //set to keep keys of item feature having desired coverage
      val usedItemFeature:HashSet[String] = filterFeatures(
                      jobInfo.jobStatus.resourceLocation_ItemFeature, 
                                                       minIFCoverage, 
                                                       sc, numItems)

      //set to keep keys of user feature having desired coverage
      val usedUserFeature:HashSet[String] = filterFeatures(
                            jobInfo.jobStatus.resourceLocation_UserFeature, 
                                                             minUFCoverage, 
                                                             sc, numItems)
    
        if (usedUserFeature.size == 0 || usedItemFeature.size == 0) {
          Logger.warn("Either user or item feature set is empty")
        }
                                                             
    //4. generate ID string 
    val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
    val resourceStr = assembleContinuousDataIden(dataHashingStr, usedUserFeature, usedItemFeature)
    
    
    val assembleFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + 
                                        "/" + resourceStr  + "_all"
    
    //check if the regression data has already generated in jobInfo.jobStatus
    //  it is possible this combination has been used (and thus generated) by other classifiers. 
    //  in that case directly return resourceStr. 
    if (! jobInfo.jobStatus.resourceLocation_AggregateData_Continuous.isDefinedAt(resourceStr)) {
        //TODO: save feature ordering in file system
        //3. perform an intersection on selected item features, generate <intersectIF>
        val itemIntersectIds = getIntersectIds(usedItemFeature, 
                                               jobInfo.jobStatus.resourceLocation_ItemFeature, sc)
                            
        //parse eligible features and extract only those with ids present in itemIntersectIds
        var (itemFeaturesRDD, itemFeatureOrder) =  getCombinedFeatures(itemIntersectIds, 
                                                        usedItemFeature, 
                                                        jobInfo.jobStatus.resourceLocation_ItemFeature, 
                                                        sc)
                                                     
        //2. perform an intersection on selected user features, generate <intersectUF>
        val userIntersectIds = getIntersectIds(usedUserFeature, 
                    jobInfo.jobStatus.resourceLocation_UserFeature, sc)
    
        //parse eligible features and extract only those with ids present in userIntersectIds
        var (userFeaturesRDD, userFeatureOrder) =  getCombinedFeatures(userIntersectIds, 
                                                        usedUserFeature, 
                                                        jobInfo.jobStatus.resourceLocation_UserFeature, 
                                                        sc)
          
        
        //below is the code to forget the lineage by saving and loading from
        //object file  can be helful while debugging
        /*
        val itemRDDPath = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/rec_spark_wsp/itemFeatRDD" 
        itemFeaturesRDD.saveAsObjectFile(itemRDDPath)
        itemFeaturesRDD = sc.objectFile(itemRDDPath)
        val userRDDPath = "hdfs://gnosis-01-01-01.crl.samsung.com:8020/user/m3.sharma/rec_spark_wsp/userFeatRDD" 
        userFeaturesRDD.saveAsObjectFile(userRDDPath)
        userFeaturesRDD = sc.objectFile(userRDDPath)
        */


        //5. perform a filtering on ( UserID, ItemID, rating) using <intersectUF> and <intersectIF>, 
        //   and generate <intersectTuple>
        //filtering such that we have only user-item pairs such that for both features have been found
        val allData = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData)
                        .map{lines => 
                            val fields = lines.split(',')
                            //user, item, watchtime
                            (fields(0), (fields(1), fields(2).toDouble))
                        }//contains both user and item in set
        

        val filterByUser = allData.join(userIntersectIds.map(x=>(x,1)))
                                  .map {x => //(user, ((item, watchtime),1))
                                     (x._2._1._1, (x._1, x._2._1._2)) //(item, (user, watchtime))
                                   }
        //filterByUser.persist
        				   
        val filterByUserItem = filterByUser.join(itemIntersectIds.map(x => (x,1)))
                                           .map { x => //(item, ((user, watchtime),1))
                                              (x._2._1._1, x._1, x._2._1._2) //(user, item, watchtime)
                                            }
        //filterByUserItem.persist
        
        //6. join features and <intersectTuple> and generate aggregated data (UF1 UF2 ... IF1 IF2 ... , feedback )
        //join with item features first as no. of items is small
        val joinedItemFeatures = filterByUserItem.map{x => (x._2, (x._1, x._3))}
                                        .join(itemFeaturesRDD) //(item, ((user, rating), IF))
                                        .map{y =>    
                                                  //(user, (item, IF, rating))
                                                  (y._2._1._1, (y._1, y._2._2, y._2._1._2))
                                        }
       
        val numExecutors = sc.getConf.getOption("spark.executor.instances")
        val numExecCores = sc.getConf.getOption("spark.executor.cores")
        val numPartitions = 2 * numExecutors.getOrElse("8").toInt * numExecCores.getOrElse("2").toInt
        //can use both range partitoner or hashpartitioner to efficiently partition by user
        //val partedByUJoinedItemFeat = joinedItemFeatures.partitionBy(new HashPartitioner(numPartitions))
        val partedByUJoinedItemFeat = joinedItemFeatures.partitionBy(new RangePartitioner(numPartitions, 
                                                                                          joinedItemFeatures)) 

        //join with user features
        val joinedUserItemFeatures = partedByUJoinedItemFeat.join(userFeaturesRDD) //(user, ((item, IF, rating), UF))
                                                            .map {x=>
                                                              //(user, item, UF, IF, rating)
                                                              x._1 + "," +
                                                              x._2._1._1 + "," +
                                                              x._2._2 + "," +
                                                              x._2._1._2 + "," +
                                                              x._2._1._3
                                                            }

        //join with item features
        val aggData = joinedUserItemFeatures
        if (jobInfo.outputResource(assembleFileName)) {
          // join features and store in assembleFileName
          aggData.saveAsTextFile(assembleFileName)
        }
        
        //7. save resource to <jobInfo.jobStatus.resourceLocation_AggregateData_Continuous>
        jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(resourceStr) =  
                    DataSet(assembleFileName, userFeatureOrder, itemFeatureOrder)
        Logger.info("assembled features: " + assembleFileName)
    }
     
      return resourceStr
  }
  
  def assembleContinuousDataIden(
      dataIdentifier:String,
      userFeature:HashSet[String], 
      itemFeature:HashSet[String]):String = {
    return "ContAggData_" + dataIdentifier+ 
          HashString.generateHash(userFeature.toString) + "_" + 
          HashString.generateHash(itemFeature.toString) 
  }
  
  def assembleBinaryData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double):String = {
      //see assembleContinuousData
    return null
  }
  
  def assembleBinaryDataIden(
      dataIdentifier:String,
      userFeature:HashSet[String], 
      itemFeature:HashSet[String]):String = {
    return "BinAggData_" + dataIdentifier + 
          HashString.generateHash(userFeature.toString) + "_" + 
          HashString.generateHash(itemFeature.toString)
  }
}
