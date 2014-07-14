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

    //get features of user or item
    def getOrderedFeatures(idSet: Set[Int], featureOrder: List[String], 
    		                    featureResourceMap: HashMap[String, FeatureStruct],
    		                    sc:SparkContext): RDD[(Int, Vector)] = {
        //broadcast idSet to workers
        val bIdSet = sc.broadcast(idSet)

    	//initialize list of RDD of format (id,features)
        var idFeatures:List[RDD[(Int, Vector)]]  = List.empty
        var featureJoin = sc.objectFile[(Int, Vector)](
                featureResourceMap(featureOrder.head).featureFileName
                ).filter(x => bIdSet.value.contains(x._1)) //id matches specified id in set

        //add remaining features
        for (usedFeature <- featureOrder.tail) {
        	featureJoin  = featureJoin.join(
        	        sc.objectFile[(Int, Vector)](featureResourceMap(usedFeature).featureFileName
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
