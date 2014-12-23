package com.samsung.vddil.recsys.mfmodel

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.data.CombinedDataSet
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.linalg.Vectors
import org.apache.spark.rdd.RDD

object MatrixFactModelPMF{
    val Param_UserProfileReg = "userProfileReg"
    val Param_ItemProfileReg = "itemProfileReg"
    val Param_Rank           = "rank"
    val Param_NumberItem     = "numIter"
    
    val defaultUserProfileReg = "0.1"
    val defaultItemProfileReg = "0.1"
    val defaultRank           = "5"
    val defaultNumberItem     = "100"
    
    val modelName = "MatrixFactModelPMF"
}

case class MatrixFactModelPMF (modelParams: HashMap[String, String]) {
	
    val paramUserProfileReg = 
        modelParams.getOrElseUpdate(MatrixFactModelPMF.Param_UserProfileReg, 
            					MatrixFactModelPMF.defaultUserProfileReg).toDouble
    val paramItemProfileReg = 
        modelParams.getOrElseUpdate(MatrixFactModelPMF.Param_ItemProfileReg, 
            					MatrixFactModelPMF.defaultItemProfileReg).toDouble
    val paramRank =
        modelParams.getOrElseUpdate(MatrixFactModelPMF.Param_Rank,
                				MatrixFactModelPMF.defaultRank).toInt
    val paramNumberItem =
        modelParams.getOrElseUpdate(MatrixFactModelPMF.Param_NumberItem, 
                				MatrixFactModelPMF.defaultNumberItem).toInt
            					
    def train(ratingData:CombinedDataSet):Option[MatrixFactModel] = {
        
    	// 1 Prepare Input
        
        val resourceStr = ""
        
        // construct Rating data structure. 
        val trainRatingDataAgg = ratingData.getDataRDD().map{
		    x => 
	        ((x._1, x._2), x._3)
		}
		.reduceByKey{_+_} 
		.map(x => Rating(x._1._1, x._1._2, x._2))
        
        // apply the spark built-in function to solve matrix factorization problem
		val model = ALS.train(trainRatingDataAgg, paramRank, paramNumberItem, paramUserProfileReg)
		
		// get user features from the matrix factorization result
		val userProfiles:RDD[(Int, Vector)] = model.userFeatures.map{x=>
		    val userIDInt = x._1
		    val userProfile:Vector = Vectors.dense(x._2)
		    (userIDInt, userProfile)
		}
		
		// get item features from the matrix factorization result
		val itemProfiles:RDD[(Int, Vector)] = model.productFeatures.map{x=>
		    val itemIDInt = x._1
		    val itemProfile:Vector = Vectors.dense(x._2)
		    (itemIDInt, itemProfile)
		}
		
		// 2 Prepare profile DUID/PID.
		val userMapping:RDD[(Int, String)] = ratingData.getUserMap().map{x=>
		    val userIDInt = x._2
		    val userID    = x._1
		    (userIDInt, userID)
		}
	
		val userProfile:RDD[(String, Vector)] = userProfiles.join(userMapping).map{x=>
		    val userID         = x._2._2
		    val userProfile    = x._2._1
		    (userID, userProfile)
		}
		
		
		val itemMapping:RDD[(Int, String)] = ratingData.getItemMap().map{x=>
		    val itemIDInt = x._2
		    val itemID    = x._1
		    (itemIDInt, itemID)
		}
		
		val itemProfile:RDD[(String, Vector)] = itemProfiles.join(itemMapping).map{x=>
		    val itemID        = x._2._2
		    val itemProfile   = x._2._1
		    (itemID, itemProfile)
		}
		
		// 3 Save an return model. 
		Some(new MatrixFactModel(
		    MatrixFactModelPMF.modelName,
			resourceStr:String,
			userProfile:RDD[(String, Vector)],
			itemProfile:RDD[(String, Vector)]
        ))
    }   
}