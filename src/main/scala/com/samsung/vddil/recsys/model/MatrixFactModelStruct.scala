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

/** This is the average profile generator, which simply provides the 
 *  average in the training data. This is used as the default cold-start 
 *  profile generator. 
 *  */
case class AverageProfileGenerator (profileRDD: RDD[Vector]) 
	extends ColdStartProfileGenerator{
    //compute average profile 
    var averageVector: Option[Vector] = None 
    
    def getProfile(feature:Any = None):Vector = {
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

