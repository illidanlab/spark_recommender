package com.samsung.vddil.recsys

import org.apache.hadoop.fs.Path

trait ResourceStruct extends Serializable{
    
    /**
     * The identity. 
     */
    def resourcePrefix: String
    
    /**
     * The identity that uniquely defines the resource. 
     */
	def resourceStr: String
	
	/**
	 * The alias of resource identity. 
	 */
	val resourceIden = resourceStr 
	
	/**
	 * The physical location of the resource 
	 */
	def resourceLoc: String
	
	
	def resourceExist():Boolean = {
        Pipeline.instance.get.fs.exists(new Path(resourceLoc))
    }
}