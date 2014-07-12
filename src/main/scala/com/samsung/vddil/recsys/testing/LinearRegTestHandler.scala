package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.feature.FeatureStruct
import com.samsung.vddil.recsys.job.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.RangePartitioner 

trait LinearRegTestHandler {

    //get features of user or item
    def getOrderedFeatures(idSet: Set[Int], featureOrder: List[String], 
    		                    featureResourceMap: HashMap[String, FeatureStruct],
    		                    sc:SparkContext): RDD[(Int, String)] = {
        
        //broadcast idSet to workers
        val bIdSet = sc.broadcast(idSet)


    	  //initialize list of RDD of format (id,features)
        var idFeatures:List[RDD[(String, String)]]  = List.empty
        var featureJoin = sc.textFile(featureResourceMap(featureOrder.head).featureFileName)
                            .map{ line =>
                                  val fields = line.split(',')
                                  val id = fields(0).toInt
                                  val features = fields.slice(1, fields.length).mkString(",")
                                  (id, features)
                             }
                            .filter(x => bIdSet.value.contains(x._1)) //id matches specified id in set
        
        //add remaining features
        for (usedFeature <- featureOrder.tail) {
        	featureJoin  = featureJoin.join( sc.textFile(featureResourceMap(usedFeature).featureFileName)
                                                .map { line =>
                                                        val fields = line.split(',')
                                                        val id = fields(0).toInt
                                                        val features = fields.slice(1, fields.length).mkString(",")
                                                        (id, features)
                                                }
                                                .filter(x => bIdSet.value.contains(x._1)) //id matches specified id in set
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
    def concateUserWAllItemFeat(userFeaturesRDD:RDD[(Int, String)],
                                itemFeaturesRDD:RDD[(Int, String)])
                                : RDD[(Int, Int, Vector)]= {
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
    def concatUserTestFeatures(userFeaturesRDD:RDD[(Int, String)],
    		                    itemFeaturesRDD:RDD[(Int, String)],
    		                    testData:RDD[Rating], sc:SparkContext) : RDD[String] = {

      val numExecutors = sc.getConf.getOption("spark.executor.instances")
      val numExecCores = sc.getConf.getOption("spark.executor.cores")
      val numPartitions = 2 * numExecutors.getOrElse("8").toInt * numExecCores.getOrElse("2").toInt 
      val joinedItemFeatures = testData.map{x =>
                          (x.item, (x.user, x.rating))
                        }.join(itemFeaturesRDD)  // (item, ((user, rating), IF))
                         .map{y =>
                          //(user, (item, IF, rating))
                          (y._2._1._1, (y._1, y._2._2, y._2._1._2))
                        }
      val partedJoinedItemFeat = joinedItemFeatures.partitionBy(
                                        new RangePartitioner(numPartitions, joinedItemFeatures))
      val userItemFeatWRating = partedJoinedItemFeat.join(userFeaturesRDD) //(user, ((item, IF, rating), UF))
                                                    .map {x=>
                                                      //(user, item, UF, IF, rating)
                                                      x._1 + "," +
                                                      x._2._1._1 + "," +
                                                      x._2._2 + "," +
                                                      x._2._1._2 + "," +
                                                      x._2._1._3
                                                    }                        
      userItemFeatWRating  
    }
    
    
    
}
