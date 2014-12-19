package com.samsung.vddil.recsys.model

import com.samsung.vddil.recsys.ResourceStruct
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.hadoop.fs.Path



class MatrixFactModel(
		val modelName:String,
		val resourceStr:String,
		private val userProfile:RDD[(String, Vector)],
		private val itemProfile:RDD[(String, Vector)],
		var coldStartUserProfiler:ColdStartProfileGenerator,
		var coldStartItemProfiler:ColdStartProfileGenerator
        ) extends ResourceStruct {
	

    def this(
    	modelName:String,
		resourceStr:String,
		userProfile:RDD[(String, Vector)],
		itemProfile:RDD[(String, Vector)]) = 
		    this(modelName:String,
        	resourceStr:String,
        	userProfile:RDD[(String, Vector)],
        	itemProfile:RDD[(String, Vector)],
        	AverageProfileGenerator (userProfile.map{_._2}), //coldStartItemProfiler
        	AverageProfileGenerator (itemProfile.map{_._2}) //coldStartUserProfiler
        )
    
    /** the name of the model, typically used as the identity prefix */
	def resourcePrefix = modelName
	def resourceLoc = "" //here resource loc should be empty
	def userProfileRDDFile:String = resourceStr + "_userProfile"
	def itemProfileRDDFile:String = resourceStr + "_itemProfile"
	
	override def resourceExist():Boolean = {
        Pipeline.instance.get.fs.exists(new Path(userProfileRDDFile)) &&
        	Pipeline.instance.get.fs.exists(new Path(itemProfileRDDFile)) 
    }
	
    /**
     * Predicts the score of an item for one single user. This method 
     * is very slow for a batch of predictions. Use other overload methods
     * instead for batch predictions. 
     */
	def predict(userId:String, itemId:String, 
	        userFeature:Option[Vector], itemFeature:Option[Vector]): Double = {
	    //find or generate user profile
	    val targetUserProfile = this.getUserProfile().filter{x => 
	        x._1.compareToIgnoreCase(userId) == 0
	    }
	    
	    val userProfile:Vector = if(targetUserProfile.count > 0){
	        targetUserProfile.first._2
	    }else{
	        this.coldStartUserProfiler.getProfile(userFeature)
	    }
	    
	    //find or generate item profile
	    val targetItemProfile = this.getItemProfile().filter{x =>
	        x._1.compareToIgnoreCase(itemId) == 0
	    }
	    
	    val itemProfile:Vector = if(targetItemProfile.count > 0) {
	        targetItemProfile.first._2
	    }else{
	        this.coldStartItemProfiler.getProfile(itemFeature)
	    }
	        
	    //generate prediction
	    userProfile.dot(itemProfile)
	}
	
	/**
	 * Predicts the score of a set of items for a single user. This method 
	 * is intended to be used when performing batch predictions for a single user. 
	 * The method is slow when there are a set of users. 
	 */
	def predict(userId:String, itemIdList:RDD[String], 
	        userFeature:Vector, itemFeatureList:RDD[(String, Vector)]): 
	        	RDD[(String, Double)] = {
	    //find or generate user profile 
	    val targetUserProfile = this.getUserProfile().filter{x => 
	        x._1.compareToIgnoreCase(userId) == 0
	    }
	    
	    val userProfile:Vector = if(targetUserProfile.count > 0){
	        targetUserProfile.first._2
	    }else{
	        this.coldStartUserProfiler.getProfile(Some(userFeature))
	    }
	    
	    //get full (item, feature) pair. 
	    val fullItemFeature = itemIdList.map{x => (x, 1)}.
	    	leftOuterJoin(itemFeatureList).map{x =>
	            val itemId                      = x._1
	            val itemFeature: Option[Vector] = x._2._2
	            (itemId, itemFeature)
	        }
	    
	    //from full (item, feature) pair, generate full (item, profile)
	    val itemProfiles = fullItemFeature.leftOuterJoin(getItemProfile()).map{x=>
	        	val itemId = x._1
	        	val itemFeatureOption = x._2._1
	        	val itemProfileOption = x._2._2
	        	val itemProfile:Vector = if(itemProfileOption.isDefined) {
	        	    	itemProfileOption.get
	        		}else{
	        		    this.coldStartItemProfiler.getProfile(itemFeatureOption)
	        		}
	        	(itemId, itemProfile)
	    	}
	    
	    // generate predictions. 
	    itemProfiles.map{x => 
	        val itemId:String      = x._1
	        val itemProfile:Vector = x._2
	        (itemId, userProfile.dot(itemProfile))
	    }
	}
	
	def predict(userIdList:RDD[String], itemIdList:RDD[String], 
	        userFeatureList:RDD[(String, Vector)], itemFeatureList:RDD[(String, Vector)]): 
	        RDD[(String, String, Double)] = {
	    
	    //get full (user, feature) pair
	    val fullUserFeature = userIdList.map{x => (x, 1)}.
	        leftOuterJoin(userFeatureList).map{x=>
	            val userId                      = x._1
	            val userFeature: Option[Vector] = x._2._2
	            (userId, userFeature)
	        }
	    
	    //from full(user, feature) pair, generate full (user, profile)
	    val userProfiles = fullUserFeature.leftOuterJoin(getUserProfile()).map{x=>
	        val userId = x._1
	        val userFeatureOption = x._2._1
	        val userProfileOption = x._2._2
	        val userProfile:Vector = if(userProfileOption.isDefined){
	            userProfileOption.get
	        }else{
	            this.coldStartUserProfiler.getProfile(userFeatureOption)
	        }
	        (userId, userProfile)
	    }
	    
	    //get full (item, feature) pair. 
	    val fullItemFeature = itemIdList.map{x => (x, 1)}.
	    	leftOuterJoin(itemFeatureList).map{x =>
	            val itemId                      = x._1
	            val itemFeature: Option[Vector] = x._2._2
	            (itemId, itemFeature)
	        }
	    
	    //from full (item, feature) pair, generate full (item, profile)
	    val itemProfiles = fullItemFeature.leftOuterJoin(getItemProfile()).map{x=>
        	val itemId = x._1
        	val itemFeatureOption = x._2._1
        	val itemProfileOption = x._2._2
        	val itemProfile:Vector = if(itemProfileOption.isDefined) {
        	    	itemProfileOption.get
        		}else{
        		    this.coldStartItemProfiler.getProfile(itemFeatureOption)
        		}
        	(itemId, itemProfile)
	    }
	    
	    //generate predictions
	    userProfiles.cartesian(itemProfiles).map{x=>
	        val userId:String      = x._1._1
	        val userProfile:Vector = x._1._2
	        val itemId:String      = x._2._1
	        val itemProfile:Vector = x._2._2
	        (userId, itemId, userProfile.dot(itemProfile))
	    }
	}
	
		
	def getUserProfile(minPartitionNum:Option[Int] = None): RDD[(String, Vector)] = {
	    if (minPartitionNum.isDefined)
            Pipeline.instance.get.sc.objectFile[(String, Vector)]( 
                    userProfileRDDFile, minPartitionNum.get)
        else
            Pipeline.instance.get.sc.objectFile[(String, Vector)](
                    userProfileRDDFile)
	}
	
	def getItemProfile(minPartitionNum:Option[Int] = None): RDD[(String, Vector)] = {
	    if (minPartitionNum.isDefined)
            Pipeline.instance.get.sc.objectFile[(String, Vector)]( 
                    itemProfileRDDFile, minPartitionNum.get)
        else
            Pipeline.instance.get.sc.objectFile[(String, Vector)](
                    itemProfileRDDFile)
	}
	
}

/** used to generate cold start item or  */
trait ColdStartProfileGenerator {
    def getProfile(feature:Option[Vector] = None):Vector
}

/** This is the average profile generator, which simply provides the 
 *  average in the training data. This is used as the default cold-start 
 *  profile generator. 
 *  */
case class AverageProfileGenerator (profileRDD: RDD[Vector]) 
	extends ColdStartProfileGenerator{
    //compute average profile 
    var averageVector: Option[Vector] = None 
    
    def getProfile(feature:Option[Vector] = None):Vector = {
        if(!averageVector.isDefined){
            //accumulation. 
            val avgPair:(Vector, Int) = profileRDD.map{
                x=>(x, 1)
            }.reduce{ (a, b)=>
                (a._1 + b._1, a._2 + b._2)
            }
            val sumVal:Int    = avgPair._2
            
            //averaging
            val avgVector:Vector = if (sumVal > 0){
                avgPair._1 / sumVal.toDouble
            }else{
                avgPair._1
            }
            
            //set average variable. 
            averageVector = Some(avgVector)
        }
        averageVector.get
    }
}

