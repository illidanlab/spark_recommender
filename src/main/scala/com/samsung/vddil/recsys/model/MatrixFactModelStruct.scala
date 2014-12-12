package com.samsung.vddil.recsys.model

import com.samsung.vddil.recsys.ResourceStruct
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.hadoop.fs.Path



class MatrixFactModel(
		val modelName:String,
		val resourceStr:String,
		private val userProfile:RDD[(String, Vector)],
		private val itemProfile:RDD[(String, Vector)],
		var coldStartItemProfiler:ColdStartProfileGenerator,
		var coldStartUserProfiler:ColdStartProfileGenerator
        ) extends ResourceStruct {
	
     //TODO: overload for default.
//    def this(
//    	modelName:String,
//		resourceStr:String,
//		userProfile:RDD[(String, Vector)],
//		itemProfile:RDD[(String, Vector)],
//    ) {
//        this(modelName:String,
//        	resourceStr:String,
//        	userProfile:RDD[(String, Vector)],
//        	itemProfile:RDD[(String, Vector)],
//        	coldStartItemProfiler:ColdStartProfileGenerator,
//        	coldStartUserProfiler:ColdStartProfileGenerator
//        )
//    }
    
    /** the name of the model, typically used as the identity prefix */
	def resourcePrefix = modelName
	def resourceLoc = "" //here resource loc should be empty
	def userProfileRDDFile:String = resourceStr + "_userProfile"
	def itemProfileRDDFile:String = resourceStr + "_itemProfile"
	
	override def resourceExist():Boolean = {
        Pipeline.instance.get.fs.exists(new Path(userProfileRDDFile)) &&
        	Pipeline.instance.get.fs.exists(new Path(itemProfileRDDFile)) 
    }
	
	def predict(UserId:String, ItemId:String): Double = {
	    throw new NotImplementedError()
	}
	
	def predict(UserId:String, ItemIdList:List[String]): List[Double] = {
	    throw new NotImplementedError()
	}
	
	def predict(UserIdList:List[String], ItemIdList:List[String]): List[Double] = {
	    throw new NotImplementedError()
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
    def getProfile(feature:Any):Vector
}

case class AverageProfileGenerator (profileRDD: RDD[Vector]) {
    //compute average profile 
    var averageVector: Option[Vector] = null 
    
    def getProfile(feature:Any = None):Vector = {
        if(!averageVector.isDefined){
            
        }
        averageVector.get
    }
}

