package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.feature.FeatureStruct
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.ResourceStruct

/**
 * This is the data structure for data
 */
trait DataStruct extends ResourceStruct{

}

/**
 * A data structure for combined data sets. 
 */
class CombinedDataSet(
        val resourceStr: String,
        val resourceLoc: String,
        val userList: CombinedDataEntityList,
        val itemList: CombinedDataEntityList,
        val userMap:  CombinedDataEntityIdMap,
        val itemMap:  CombinedDataEntityIdMap,
        val dates: Array[String]
        ) extends DataStruct{
    
    val resourcePrefix = "CombinedData"
    
}

/**
 * Store the list of users/items in the combined data set
 */
case class CombinedDataEntityList(
        listObj:Array[String],
        listLoc: String
    ){
    
    def size = listObj.size
}

/**
 * Stores the list of user mapping/item mapping in the combined data sets
 */
case class CombinedDataEntityIdMap(
        mapObj: Map[String, Int],
        mapLoc: String
    ){
    
    def size = mapObj.size
}


/**
 * This is the data used to store an assembled feature, which includes 
 * features and their orders used in assembling them.  
 * 
 * @param location Resource location
 * @param userFeatureOrder Order of user feature, each element is the resource identity of a specific user feature
 * @param itemFeatureOrder Order of item feature, each element is the resource identity of a specific user feature
 */
class AssembledDataSet(
    val resourceStr: String,
    val resourceLoc: String, 
    val userFeatureOrder: List[FeatureStruct],
    val itemFeatureOrder: List[FeatureStruct]
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
        new AssembledDataSet(resourceIden, resourceLoc, userFeatureOrder, itemFeatureOrder)
    }
}

case class DataSplit(
        training:AssembledDataSet,
        testing:AssembledDataSet,
        validation:AssembledDataSet
     )


//class SplittedAssembledDataSet(
//        override val resourceIden: String,
//        override val resourceLoc: String, 
//        override val userFeatureOrder: List[FeatureStruct],
//        override val itemFeatureOrder: List[FeatureStruct],
//        val parentData: AssembledDataSet
//        ) extends AssembledDataSet(resourceIden, resourceLoc, userFeatureOrder, itemFeatureOrder){
//    
//}