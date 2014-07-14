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
        //initialize list of RDD of format (id,features)
        var idFeatures:List[RDD[(String, Vector)]]  = List.empty
        var featureJoin = sc.objectFile[(String, Vector)](
                featureResourceMap(featureOrder.head).featureFileName
                ).filter(x => idSet.contains(x._1)) //id matches specified id in set
        
        for (usedFeature <- featureOrder.tail) {
        	featureJoin  = featureJoin.join(
        	        sc.objectFile[(String, Vector)](featureResourceMap(usedFeature).featureFileName
        	                ).filter(x => idSet.contains(x._1))
        	        ).map{x =>
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
            userItemFeatureWithRating:RDD[(String, String, Vector, Double)]):
            RDD[(String, String, LabeledPoint)] = {
        userItemFeatureWithRating.map { tuple =>
            val userID = tuple._1
            val itemID = tuple._2
            val features = tuple._3
            val rating = tuple._4
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
    def concateUserWAllItemFeat(userFeaturesRDD:RDD[(String, Vector)],
                                itemFeaturesRDD:RDD[(String, Vector)])
                                : RDD[(String, String, SV)]= {
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
    }
    
}
