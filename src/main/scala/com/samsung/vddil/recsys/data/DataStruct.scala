package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.feature.FeatureStruct
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.ResourceStruct
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.Pipeline

/**
 * This is the data structure for data
 */
trait DataStruct extends ResourceStruct{

}

/**
 * A data structure for combined data sets. 
 * 
 * @param resourceStr the unique identity of the dataset 
 *        resourcePrefix + data hash string
 * @param resourceLoc the location the dataset is stored. 
 * @param userList a list of users
 * @param itemList a list of items 
 * @param userMap mapping from user ID string to integer
 * @param itemMap mapping from item ID string to integer
 * @param dates the dates from which the combined dataset is generated. 
 */
class CombinedDataSet(
        val resourceStr: String,
        val resourceLoc: String,
        val userListLoc: String,
        val itemListLoc: String,
        val userMapLoc: String,
        val itemMapLoc: String,
        val userNum: Int,
        val itemNum: Int,
        val recordNum: Long,
        val dates: Array[String]
        ) extends DataStruct{
    
    val resourcePrefix = CombinedDataSet.resourcePrefix
    
    def getDataRDD(minPartitionNum:Option[Int] = None): RDD[(Int, Int, Double)] = {
        if (minPartitionNum.isDefined)
            Pipeline.instance.get.sc.textFile(resourceLoc, minPartitionNum.get).map{line => 
            	val splits = line.split(",")
            	val userId = line(0).toInt
            	val itemId = line(1).toInt
            	val rating    = line(2).toDouble
            	(userId, itemId, rating)
        	}
        	
        else
            Pipeline.instance.get.sc.textFile(resourceLoc).map{line =>
            	val splits = line.split(",")
            	val userId = line(0).toInt
            	val itemId = line(1).toInt
            	val rating    = line(2).toDouble
            	(userId, itemId, rating)
        	}
    }
    
    def getUserList(minPartitionNum:Option[Int] = None): RDD[String] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.textFile(userListLoc, minPartitionNum.get)
        else
            Pipeline.instance.get.sc.textFile(userListLoc)
    }
    
    def getItemList(minPartitionNum:Option[Int] = None): RDD[String] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.textFile(itemListLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.textFile(itemListLoc)
    }
    
    def getUserMap(minPartitionNum:Option[Int] = None): RDD[(String, Int)] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.objectFile[(String, Int)](userMapLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.objectFile[(String, Int)](userMapLoc)
    }
    
    def getItemMap(minPartitionNum:Option[Int] = None): RDD[(String, Int)] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.objectFile[(String, Int)](itemMapLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.objectFile[(String, Int)](itemMapLoc)
    }
}

object CombinedDataSet{
    val resourcePrefix = "CombinedData"
}

/**
 * This is the data used to store an assembled feature, which includes 
 * features and their orders used in assembling them.  
 * 
 * @param resourceStr Resource string
 * @param resourceloc Resource location
 * @param userFeatureOrder Order of user feature, each element is the resource identity of a specific user feature
 * @param itemFeatureOrder Order of item feature, each element is the resource identity of a specific user feature
 * @param size the size of current dataset (-1 means does not check).  
 * 
 */
class AssembledDataSet(
    val resourceStr: String,
    val resourceLoc: String, 
    val userFeatureOrder: List[FeatureStruct],
    val itemFeatureOrder: List[FeatureStruct],
    val combData: CombinedDataSet,
    var size: Long = -1
    ) extends DataStruct{
    
    val resourcePrefix = "AssembledData"
    
    /**
     * Splitting information e.g, ("test_001"->DataSplitting, "test_002"->DataSplitting) 
     */
    val splittings:HashMap[String, DataSplit] = HashMap()
    
    /**
     * Set a splitting of this dataset.
     */
    def putSplit(splitName: String, 
            tr: AssembledDataSet, 
            te: AssembledDataSet, 
            va: AssembledDataSet):Unit = {
        
        putSplit(splitName, DataSplit(tr, te, va))
    }
    
    /**
     * Set a splitting of this dataset.
     */
    def putSplit(splitName: String, split: DataSplit ):Unit = {
        if(splittings.isDefinedAt(splitName)){
            Logger.warn(s"The splitting $splitName already exists for $resourceIden, will be overwritten." )
        }
        
        splittings(splitName) = split
    }
    
    /**
     * Get a splitting from this dataset.
     */
    def getSplit(splitName: String):Option[DataSplit] = {
        if (splittings.isDefinedAt(splitName))
        	Some(splittings(splitName))
        else
            None
    }
    
    def createSplitStruct(resourceIden:String, resourceLoc:String): AssembledDataSet = {
        new AssembledDataSet(resourceIden, resourceLoc, userFeatureOrder, itemFeatureOrder, combData)
    }
}

/**
 * Stores the splits of an AssembledDataset
 */
case class DataSplit(
        training:AssembledDataSet,
        testing:AssembledDataSet,
        validation:AssembledDataSet
     )

