package com.samsung.vddil.recsys.mfmodel

import com.samsung.vddil.recsys.ResourceStruct
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.hadoop.fs.Path
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureStruct


/**
 * The matrix factorization model. The model requires two RDDs of user and item profiles. 
 * It also requires to specify cold start profile generators for both user and item.   
 * 
 * TODO: add the support of user bias and item bias. 
 * 
 * @param modelName
 * @param resourceStr
 * @param userProfile an RDD of (user ID string, user profile)
 * @param itemProfile an RDD of (item ID string, item profile) 
 * @param coldStartUserProfiler an implementation of ColdStartProfileGenerator for user profile 
 * @param coldStartItemProfiler an implementation of ColdStartProfileGenerator for item profile 
 */
class MatrixFactModel(
		val modelName:String,
		val resourceStr:String, //an identity string
		val resourceLoc:String, //the physical location (prefix) .
		var modelParams:HashMap[String, String],
		private val userProfile:RDD[(String, Vector)],
		private val itemProfile:RDD[(String, Vector)],
		var coldStartUserProfiler:ColdStartProfileGenerator,
		var coldStartItemProfiler:ColdStartProfileGenerator
        ) extends ResourceStruct {
	
    //constructing process
    saveUserProfile(userProfile)
    saveItemProfile(itemProfile)
    
    //saveUserProfilePlain(userProfile)
    //saveItemProfilePlain(itemProfile)
    
	/**
	 * Constructs a matrix factorization model using average profile generators
	 * for both user profile and item profile. 
	 */
    def this(
    	modelName:String,
		resourceStr:String,
		resourceLoc:String,
		modelParams:HashMap[String, String],
		userProfile:RDD[(String, Vector)],
		itemProfile:RDD[(String, Vector)]
		) = 
		    this(modelName:String,
	        	resourceStr:String,
	        	resourceLoc:String,
	        	modelParams:HashMap[String, String],
	        	userProfile:RDD[(String, Vector)],
	        	itemProfile:RDD[(String, Vector)],
	        	AverageProfileGenerator (userProfile.map{_._2}), //coldStartItemProfiler
	        	AverageProfileGenerator (itemProfile.map{_._2}) //coldStartUserProfiler
		    )
    
    /** the name of the model, typically used as the identity prefix */
	def resourcePrefix = modelName
	/** stores user profile RDD[(String, Vector)]*/
	def userProfileRDDFile:String = resourceLoc + "_userProfile"
	/** stores item profile RDD[(String, Vector)]*/
	def itemProfileRDDFile:String = resourceLoc + "_itemProfile"
	
	/** if resource exists. */
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
	
	/**
	 * Predicts all (user, item) ratings, given the 
	 * Cartesian product between a given user ID list 
	 * and a given item ID list. 
	 */
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
	
	/**
	 * Predicts all (user, item) appeared in the 
	 * predictData input. The operation is optimized 
	 * against this use case, and is much faster than 
	 * other methods provided above. 
	 * 
	 * @param predictData is 
	 *        RDD[(UserID:String, ItemID:String, anyValue: MetaType)]
	 * @param userFeatureList a list of user features (for those have user features)
	 * @param itemFeatureList a list of item features (for those have item features)
	 * @param userIdListOpt an option of user ID list 
	 * @param itemIdListOpt an option of item ID list
	 * 
	 * Output is RDD[(userId:String, itemID:String, 
	 *               prediction:Double, anyValue: MetaType)]
	 */
	def predict[MetaType](
	        predictData:RDD[(String, String, MetaType)], 
	        userFeatureList:RDD[(String, Vector)], 
	        itemFeatureList:RDD[(String, Vector)],
	        userIdListOpt:Option[RDD[String]] = None,
	        itemIdListOpt:Option[RDD[String]] = None):
	        RDD[(String, String, Double, MetaType)]= {
	    
	    //PART I. User Profile
	    val userIdList:RDD[String] = if(userIdListOpt.isDefined){
	        userIdListOpt.get
	    }else{
	        predictData.map{x=>(x._1, 1)}.
	        			reduceByKey{(x1, x2)=>x1}.
	        			map{x=>x._1}
	    }
	    
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
	    
	    
	    //PART II. Item Profile
	    val itemIdList:RDD[String] = if(itemIdListOpt.isDefined){
	        itemIdListOpt.get
	    }else{
	        predictData.map{x=>(x._2, 1)}.
	        			reduceByKey{(x1, x2)=>x1}.
	        			map{x=>x._1}
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
	    
	    //join with User.Item profile
	    predictData.map{x => 
	        val userIdStr:String = x._1
	        val itemIdStr:String = x._2
	        val mData:MetaType  = x._3
	        (userIdStr, (itemIdStr, mData))
	    }.join(userProfile).map{ x=>
	        val userIdStr:String   = x._1
	        val userProfile:Vector = x._2._2
	        val itemIdStr:String   = x._2._1._1
	        val mData:MetaType     = x._2._1._2
	        (itemIdStr, (userIdStr, userProfile, mData))
	    }.join(itemProfile).map{ x=>
	        val userIdStr:String   = x._2._1._1
	        val userProfile:Vector = x._2._1._2
	        val itemIdStr:String   = x._1
	        val itemProfile:Vector = x._2._2
	        val mData:MetaType     = x._2._1._3
	        
	        val predictScore:Double = userProfile.dot(itemProfile)
	        
	        (userIdStr, itemIdStr, predictScore, mData)
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
	
	private def saveUserProfile(userProfileRDD: RDD[(String, Vector)]) = {
	    if(! Pipeline.instance.get.fs.exists(new Path(userProfileRDDFile)))
	    	userProfileRDD.saveAsObjectFile(userProfileRDDFile)
	}
	
	private def saveItemProfile(itemProfileRDD: RDD[(String, Vector)]) = {
	    if(! Pipeline.instance.get.fs.exists(new Path(itemProfileRDDFile)))
	    	itemProfileRDD.saveAsObjectFile(itemProfileRDDFile)
	}
	
	private def saveUserProfilePlain(userProfileRDD: RDD[(String, Vector)]) = {
	    val userProfileRDDPlainFile = userProfileRDDFile + ".plain"
	    
	    if(! Pipeline.instance.get.fs.exists(new Path(userProfileRDDPlainFile)))
	    	userProfileRDD.map{x=>
	        	x._1 + "\t" + x._2
	    	}.saveAsTextFile(userProfileRDDPlainFile)
	}
	
	private def saveItemProfilePlain(itemProfileRDD: RDD[(String, Vector)]) = {
	    val itemProfileRDDPlainFile = itemProfileRDDFile + ".plain"
	    
	    if(! Pipeline.instance.get.fs.exists(new Path(itemProfileRDDPlainFile)))
	    	itemProfileRDD.map{x=>
	        	x._1 + "\t" + x._2
	    	}.saveAsTextFile(itemProfileRDDPlainFile)
	}
}

case class MatrixFactModelMetaInfo(
		val modelName:String,
		val resourceStr:String, //an identity string
		val resourceLoc:String, //the physical location (prefix) .
		var modelParams:HashMap[String, String],
		val userFeatureOrder: List[FeatureStruct],
		val itemFeatureOrder: List[FeatureStruct]
        ){
    
}


