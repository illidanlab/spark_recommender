package com.samsung.vddil.recsys.model


object ModelUtil {
	
    
    /*
     * will parse parameter string to give parameters
     * "0:2:8,10,12" -> [0,2,4,6,8,10,12]
     */
    def parseParamString(param: String):Array[Double] = {
        param.split(",").map(_.split(":")).flatMap { _ match {
                //Array(0,2,8)
                case Array(a,b,c) => a.toDouble to c.toDouble by b.toDouble 
                //Array(10) or Array(12)
                case Array(a) => List(a.toDouble)
            }
        }
    }
    
    
}