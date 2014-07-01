package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.job.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, Vector}


trait LinearRegTestHandler {

    //get features of user or item
    def getOrderedFeatures(idSet: Set[String], featureOrder: List[String], 
    		                    featureResourceMap: HashMap[String, FeatureStruct],
    		                    sc:SparkContext): RDD[(String, String)] = {
    	//initialize list of RDD of format (id,features)
        var idFeatures:List[RDD[(String, String)]]  = List.empty
        var featureJoin = sc.textFile(featureResourceMap(featureOrder.head).featureFileName)
                            .map{ line =>
                                  val fields = line.split(',')
                                  val id = fields(0)
                                  val features = fields.slice(1, fields.length).mkString(",")
                                  (id, features)
                             }
                            .filter(x => idSet.contains(x._1)) //id matches specified id in set
        
         //add remaining features
        for (usedFeature <- featureOrder.tail) {
        	featureJoin  = featureJoin.join( sc.textFile(featureResourceMap(usedFeature).featureFileName)
                                                .map { line =>
                                                        val fields = line.split(',')
                                                        val id = fields(0)
                                                        val features = fields.slice(1, fields.length).mkString(",")
                                                        (id, features)
                                                }
                                                .filter(x => idSet.contains(x._1)) //id matches specified id in set
                                            ).map {x =>
                                                (x._1, x._2._1 + "," + x._2._2)
                                            }
        }
                            
        featureJoin       
    }
    
    
    //convert user item features to LebeledPoint
    def convToLabeledPoint(userItemFeatureWRating:RDD[String]):RDD[(String, String, LabeledPoint)] = {
    	userItemFeatureWRating.map { line =>
            //(U,I,UF[],IF[], rating)
            val parts = line.split(',')
            val rating = parts(parts.length - 1).toDouble
            //features start from 3rd index
            val features = parts.slice(2, parts.length -1).map(_.toDouble)
            //(U, I, RDD[LabeledPoint])
            (parts(0), parts(1), LabeledPoint(rating, Vectors.dense(features)))
       }
    }
    
    
    //generate all possible user item pairs with feature vectors
    def concateUserWAllItemFeat(userFeaturesRDD:RDD[(String, String)],
                                itemFeaturesRDD:RDD[(String, String)])
                                : RDD[(String, String, Vector)]= {
        val userItemFeatures = userFeaturesRDD.cartesian(itemFeaturesRDD) //((U,UF)(I,IF))
                                              .map { x =>
                                                  (x._1._1, x._2._1, x._1._2, x._2._2)
                                              } // (U, I, UF, IF)
        val userItemFeatVector = userItemFeatures.map {x => 
        	    val user = x._1
        	    val item = x._2
        	    //join user and item features
        	    val features = x._3.split(',') ++ x._4.split(',')
        	    val featureVec = Vectors.dense(features.map{_.toDouble})
        	    //U, I, Feature vector
        	    (user, item, featureVec)
        }
        
        userItemFeatVector
    }
    
    
    //get concatenated features with test data
    //user, item, UF, IF, rating
    def concatUserTestFeatures(userFeaturesRDD:RDD[(String, String)],
    		                    itemFeaturesRDD:RDD[(String, String)],
    		                    testData:RDD[Rating]) : RDD[String] = {
    	val userItemFeatureWRating  = testData.map {x => 
    		        (x.user, (x.item, x.rating))
    		    }
                .join(userFeaturesRDD) // (user, ((item, rating), UF)) 
                .map {y =>
                      //(item, (user, UF, rating))
                      (y._2._1._1, (y._1, y._2._2, y._2._1._2)) 
                }
                .join(itemFeaturesRDD) //(item, ((user, UF, rating), IF))
                .map {z =>
                        //user, item, UF, IF, rating
                        z._2._1._1 + "," + z._1 + "," + 
                        z._2._1._2 + "," + z._2._2 + 
                        "," + z._2._1._3
                        //UF, IF, rating
                        //z._2._1._2 + "," + z._2._2 + "," 
                        //+ z._2._1._3
                }
        
        userItemFeatureWRating  
    }
    
    
    
}
