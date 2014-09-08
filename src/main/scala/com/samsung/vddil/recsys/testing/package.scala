package com.samsung.vddil.recsys

import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import com.samsung.vddil.recsys.feature.ItemFeatureHandler
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import com.samsung.vddil.recsys.feature.FeatureStruct
import org.apache.spark.RangePartitioner
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.data.CombinedDataSet

/**
 * The testing package includes a set of test units. Each test unit 
 * is a work flow that defines the testing scenario, e.g., on which set of 
 * users the test is being done. Also each test unit supports the 
 * computation of a set of metrics.  
 */
package object testing {
	
    /**
	 * remove new users and items from test
	 * 
	 * NOTE: the implementation of this function needs to be improved. 
	 *       the rating data structure may need to be removed. 
	 */
	def filterTestRatingData(testData: RDD[Rating], trainCombData:CombinedDataSet,
			                    sc:SparkContext): RDD[Rating] = {
		var filtTestData:RDD[Rating] = testData  
    
	    //get userMap and itemMap
	    val userIdMapRDD = trainCombData.getUserMap() 
	    val itemIdMapRDD = trainCombData.getItemMap()
	    
	    testData.map{line =>
		    (line.user, (line.item, line.rating))
		}.join(
		    userIdMapRDD.map{line => (line._2, line._1)}
	    ).map{line => //(userIdInt, ((itemIdInt, rating), userIdStr))
	        val userIdInt:Int = line._1
	        val itemIdInt:Int = line._2._1._1
	        val rating:Double = line._2._1._2
		    (itemIdInt, (userIdInt, rating))
		}.join(
			itemIdMapRDD.map{line => (line._2, line._1)}
		).map{line => //(itemIdInt, ((userIdInt, rating), itemIdStr) )
		    val userIdInt:Int = line._2._1._1
		    val itemIdInt:Int = line._1
		    val rating:Double = line._2._1._2
		    Rating(userIdInt, itemIdInt, rating)
		}
		
  }
    
  /**
   * get new items not seen during training from test
   * @param testData RDD of ratings in test data
   * @param trainItems contains set of train items
   * @param sc spark context
   * @return set of new items not appeared in training
   */
  def getColdItems(testData:RDD[(String, String, Double)], trainItems:Set[String], 
    sc:SparkContext): Set[String] = {
  
    //broadcast trainItems
    val bTrItems = sc.broadcast(trainItems)

    //get test items
    val testItems:Set[String] = testData.map(_._2 //item string id
                                            ).filter(
                                              item => !(bTrItems.value(item))
                                            ).distinct.collect.toSet
    testItems
  }

  /**
   * return features for passed items  
   * @param items set of items for which we need to generate feature
   * @param jobInfo
   * @param featureOrder
   * @param featureSources
   */
  def getColdItemFeatures(items:Set[String], jobInfo:RecJob,
    featureOrder:List[ItemFeatureStruct], dates:List[String]
    ):RDD[(String, Vector)] = {
    
    //get feature resource location map
    val featureResourceMap = jobInfo.jobStatus.resourceLocation_ItemFeature  
    
    //get spark context
    val sc = jobInfo.sc

    val itemFeatures:List[RDD[(String, Vector)]] = featureOrder.map{feature =>
      val itemFeatureExtractor:ItemFeatureExtractor =
           feature.extractor
        
      val featMapFileName:String =
           featureResourceMap(feature.resourceStr).featureMapFileName
        
      val featParams = itemFeatureExtractor.trFeatureParams
      val featureSources = itemFeatureExtractor.getFeatureSources(dates, jobInfo)
      itemFeatureExtractor.extractFeature(items, featureSources, featParams,
        featMapFileName, sc)
    }

    //combine feature in order
    val headItemFeatures:RDD[(String, Vector)] = itemFeatures.head 
    val combItemFeatures:RDD[(String, Vector)] =
      itemFeatures.tail.foldLeft(headItemFeatures){ (itemFeat1, itemFeat2) =>
        val joinedItemFeat:RDD[(String, (Vector, Vector))] = itemFeat1.join(itemFeat2)
        joinedItemFeat.mapValues{featVecs =>
          featVecs._1 ++ featVecs._2
        }
      }

    combItemFeatures
  }
  
   /**
     *  Concatenate features according to a given order. 
     * 
     *  @param idSet ids for which features need to be generated
     *  @param featureOrder order in which features will be generated
     *  @param featureResourceMap location of features
     *  @param sc SparkContext
     *  @param isPartition whether to partition the loaded feature file helpful
     *  in case of large no. of features
     *  @return (ID:String, feature:com.samsung.vddil.recsys.linalg.Vector)
     */

    //get features of user or item
    def getOrderedFeatures(idSet: RDD[Int], featureOrder: List[FeatureStruct], 
    		                    sc:SparkContext, isPartition:Boolean = false): RDD[(Int, Vector)] = {
      
      val partitionNum = Pipeline.getPartitionNum()
        
      //create parallel RDDs of ids to be used in join
      val idRDDs = idSet.map((_,1))

    	//initialize list of RDD of format (id,features)
      val headFeatures = sc.objectFile[(Int, Vector)](featureOrder.head.featureFileName)
      
      Logger.info("Starting partitioning features...") 
      val partedFeatures = if(isPartition) {
                            headFeatures.partitionBy(new
                              RangePartitioner(partitionNum, idRDDs)) 
                           } else headFeatures
                           
      Logger.info("Features partitioned successfully, joining features...")
      var featureJoin = partedFeatures.join(idRDDs).map{x => 
                                                      val id:Int = x._1
                                                      val feature:Vector = x._2._1
                                                      (id, feature)
                                                    }
      //add remaining features
      for (usedFeature <- featureOrder.tail) {
        featureJoin  = featureJoin.join(
                          sc.objectFile[(Int, Vector)](usedFeature.featureFileName)
                        ).map{x => // (ID, (prevFeatureVector, newFeatureVector))
                            val ID = x._1
                            val feature:Vector = x._2._1 ++ x._2._2
                            (ID, feature)
                        }
      }
      Logger.info("Feature joining completed...")
      featureJoin
    }
    
    /**
     * Converts the tuples into MLLib format.
     * 
     * @param userItemFeatureWithRating
     */
    def convToLabeledPoint(
            userItemFeatureWithRating:RDD[(Int, Int, Vector, Double)]
            ):RDD[(Int, Int, LabeledPoint)] = {
        userItemFeatureWithRating.map { tuple =>
            val userID:Int = tuple._1
            val itemID:Int = tuple._2
            val features:Vector = tuple._3
            val rating:Double = tuple._4
            (userID, itemID, LabeledPoint(rating, features.toMLLib))
        }
    }
    
    /**
     * Concatenate all possible user item pairs with feature vectors 
     * 
     * @param userFeatureRDD
     * 
     * @param itemFeatureRDD
     * 
     */
    def concateUserWAllItemFeat(
            userFeaturesRDD:RDD[(Int, Vector)],
            itemFeaturesRDD:RDD[(Int, Vector)]
            ): RDD[(Int, Int, SV)]= {
        val userItemFeatures = 
            userFeaturesRDD.cartesian(itemFeaturesRDD).map{ x=> //((userID, userFeature), (itemID, itemFeature))
                val userID:Int = x._1._1
                val itemID:Int = x._2._1
                val feature:Vector = x._1._2 ++ x._2._2
                (userID, itemID, feature.toMLLib)
            }
        userItemFeatures
    }
    
    
    /**
     * This is the object version of concatUserTestFeatures,
     * which returns (userID, itemID, feature:userFeature++itemFeature, rating)
     * 
     * @param userFeaturesRDD
     * @param itemFeaturesRDD
     * @param testData
     */
    def concatUserTestFeatures(userFeaturesRDD:RDD[(Int, Vector)],
    		                    itemFeaturesRDD:RDD[(Int, Vector)],
    		                    testData:RDD[Rating]) : RDD[(Int, Int, Vector, Double)] = {
        
    	val userItemFeatureWithRating = testData.map{ x=>
            (x.user, (x.item, x.rating))
        }.join(userFeaturesRDD).map{ y=> //(userID, ((itemID, rating), UF))
            val userID:Int = y._1
            val itemID:Int = y._2._1._1
            val userFeature:Vector = y._2._2
            val rating:Double = y._2._1._2
            (itemID, (userID, userFeature, rating))
        }.join(itemFeaturesRDD).map{ z=> //(itemID, ((userID, UF, rating), IF))
            val userID:Int = z._2._1._1
            val itemID:Int = z._1
            val feature:Vector = z._2._1._2 ++ z._2._2 
            val rating:Double = z._2._1._3
            (userID, itemID, feature, rating)
        }
        
        userItemFeatureWithRating	
    }
    
}