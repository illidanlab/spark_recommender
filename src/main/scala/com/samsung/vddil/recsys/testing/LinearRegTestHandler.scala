package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.job.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors => SVs, Vector => SV}
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.RangePartitioner 
import com.samsung.vddil.recsys.Pipeline

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
    def getOrderedFeatures(idSet: Set[String], featureOrder: List[String], 
    		                    featureResourceMap: HashMap[String, FeatureStruct],
    		                    sc:SparkContext): RDD[(String, Vector)] = {
        
        //broadcast idSet to workers
        val bIdSet = sc.broadcast(idSet)
        
        //initialize list of RDD of format (id,features)
        var idFeatures:List[RDD[(String, Vector)]]  = List.empty
        var featureJoin = sc.objectFile[(String, Vector)](
                featureResourceMap(featureOrder.head).featureFileName
                ).filter(x => bIdSet.value.contains(x._1)) //id matches specified id in set

        //add remaining features
        for (usedFeature <- featureOrder.tail) {
        	featureJoin  = featureJoin.join(
        	        sc.objectFile[(String, Vector)](featureResourceMap(usedFeature).featureFileName
        	                ).filter(x => bIdSet.value.contains(x._1)) //id matches specified id in set
        	        ).map{x => // (ID, (prevFeatureVector, newFeatureVector))
        	            val ID = x._1
        	            val feature:Vector = x._2._1 ++ x._2._2
        	            (ID, feature)
        	        }
        }
        featureJoin
    }
    
    /**
     * Converts the tuples into MLLib format.
     * 
     * @param userItemFeatureWithRating
     */
    def convToLabeledPoint(
            userItemFeatureWithRating:RDD[(String, String, Vector, Double)]
            ):RDD[(String, String, LabeledPoint)] = {
        userItemFeatureWithRating.map { tuple =>
            val userID:String = tuple._1
            val itemID:String = tuple._2
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
            userFeaturesRDD:RDD[(String, Vector)],
            itemFeaturesRDD:RDD[(String, Vector)]
            ): RDD[(String, String, SV)]= {
        val userItemFeatures = 
            userFeaturesRDD.cartesian(itemFeaturesRDD).map{ x=> //((userID, userFeature), (itemID, itemFeature))
                val userID:String = x._1._1
                val itemID:String = x._2._1
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
    def concatUserTestFeatures(userFeaturesRDD:RDD[(String, Vector)],
    		                    itemFeaturesRDD:RDD[(String, Vector)],
    		                    testData:RDD[Rating]) : RDD[(String, String, Vector, Double)] = {
        
    	val userItemFeatureWithRating = testData.map{ x=>
            (x.user, (x.item, x.rating))
        }.join(userFeaturesRDD).map{ y=> //(userID, ((itemID, rating), UF))
            val userID:String = y._1
            val itemID:String = y._2._1._1
            val userFeature:Vector = y._2._2
            val rating:Double = y._2._1._2
            (itemID, (userID, userFeature, rating))
        }.join(itemFeaturesRDD).map{ z=> //(itemID, ((userID, UF, rating), IF))
            val userID:String = z._2._1._1
            val itemID:String = z._1
            val feature:Vector = z._2._1._2 ++ z._2._2 
            val rating:Double = z._2._1._3
            (userID, itemID, feature, rating)
        }
        
        userItemFeatureWithRating	
    		
//    	val numPartitions = Pipeline.getPartitionNum
//    	val joinedItemFeatures = testData.map{ x=>
//    	    (x.user, (x.item, x.rating))
//    	}.join(itemFeaturesRDD).map{ y=> //(item, ((user, rating), IF))
//    	    val userID:String = y._2._1._1 
//    	    val itemID:String = y._1
//    	    val itemFeature:Vector = y._2._2
//    	    val rating:Double = y._2._1._2
//    	    (userID, (itemID, itemFeature, rating))
//    	}
//    	
//    	// use range partition to reparition the data for better performance.
//    	val partedJoinedItemFeat = joinedItemFeatures.partitionBy(
//                                        new RangePartitioner(numPartitions, joinedItemFeatures))
//    	
//        val userItemFeatureWithRating = partedJoinedItemFeat.
//        		join(userFeaturesRDD).map {x=> //(user, ((item, IF, rating), UF))
//					val userID = x._1 
//					val itemID = x._2._1._1 
//					val userFeature:Vector = x._2._2
//					val itemFeature:Vector = x._2._1._2 
//					val rating:Double = x._2._1._3
//					//(user, item, UF++IF, rating)
//					(userID, itemID, userFeature++itemFeature, rating)
//                }                                
//
//        userItemFeatureWithRating
    }
    
}
