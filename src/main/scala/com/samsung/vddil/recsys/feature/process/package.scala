/**
 *
 */
package com.samsung.vddil.recsys.feature

/**
 * This package includes definitions for feature transformation processes, 
 * including feature selection, dimension reduction, as well as, normalization
 * 
 * @author jiayu.zhou
 *
 */
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.linalg.SparseVector
import com.samsung.vddil.recsys.linalg.DenseVector
package object process {
    
    /**
     * Generate the new feature map by specifying certain original positions to be removed.
     * This operation involves sorting on the removalPositions. 
     * 
     * result => (old position, new position) 
     */
	def genMapByRemovalPos(featureDimension:Int, removalPositions: List[Int]):Map[Int, Int] = {
		// sort the removal positions. 
	    val sortedRemovalList = removalPositions.sorted
	    var pointer = 0
	    var resultMap = Map[Int, Int]()
	    
	    (0 until featureDimension).foreach{ number =>
	        if( pointer < sortedRemovalList.length && 
	                number == sortedRemovalList(pointer)) {
	            pointer += 1 
	        }else{
	            resultMap += (number -> (number - pointer))
	        }
	    }
	    
	    resultMap
	}
	
	/**
	 * Generate the new feature map by specifying the original. 
	 * 
	 * result => (old position, new position) 
	 */
	def genMapByInclusionPos(
	        featureDimension:Int, 
	        inclusionPosition: List[Int]):Map[Int, Int] = {
	    
	    (0 until inclusionPosition.size).
	    		zip(inclusionPosition).map{line =>
	        val newIdx = line._1
	        val oldIdx = line._2
	        (oldIdx, newIdx)
	    }.toMap
	    
	}
	
	/**
	 * 
	 */
	def mapFeatureVector(
	        inclusionPosition: List[Int], 
	        reduceVector: Vector): Vector = {
	    
	    reduceVector.slice(inclusionPosition)
	}
	
	/**
	 * 
	 */
	def mapFeatureMap(
	        featureDimension:Int, 
	        inclusionPosition: List[Int], 
	        mapRDD: RDD[(Int, String)]): 
	        RDD[(Int, String)] = {
	    
	    val removalMap:Map[Int, Int] =  genMapByInclusionPos(featureDimension:Int, inclusionPosition: List[Int])
	    
	    mapFeatureMap(
	        removalMap:Map[Int, Int], 
	        mapRDD: RDD[(Int, String)])
	    
	}
	
	/**
	 * 
	 */
	def mapFeatureMap(
	        removalMap:Map[Int, Int], 
	        mapRDD: RDD[(Int, String)]): 
	        RDD[(Int, String)] = {
	    
	    mapRDD.filter{mapEntry =>
	        removalMap.isDefinedAt(mapEntry._1)
	    }.map{mapEntry =>
	        (removalMap(mapEntry._1), mapEntry._2)
	    }
	}
}