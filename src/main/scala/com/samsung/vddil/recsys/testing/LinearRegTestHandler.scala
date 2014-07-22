package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.job.Rating
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.RangePartitioner 
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap

trait LinearRegTestHandler {
	
    /**
     *  Concatenate features according to a given order. 
     * 
     *  @param idSet
     *  @param featureOrder
     *  @param featureResourceMap
     *  @param sc
     *  
     *  @return (ID:String, feature:com.samsung.vddil.recsys.linalg.Vector)
     */

    //get features of user or item
    def getOrderedFeatures(idSet: RDD[Int], featureOrder: List[String], 
    		                    featureResourceMap: HashMap[String, FeatureStruct],
    		                    sc:SparkContext, isPartition:Boolean = false): RDD[(Int, Vector)] = {
      
      //create parallel RDDs of ids to be used in join
      val idRDDs = idSet.map((_,1))

    	//initialize list of RDD of format (id,features)
      val headFeatures = sc.objectFile[(Int, Vector)](
                          featureResourceMap(featureOrder.head).featureFileName) 
      Logger.info("Starting partitioning features...") 
      val partedFeatures = if(isPartition) {
                            headFeatures.partitionBy(new
                              RangePartitioner(Pipeline.getPartitionNum,
                                                idRDDs)) 
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
                          sc.objectFile[(Int, Vector)](featureResourceMap(usedFeature).featureFileName)
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
