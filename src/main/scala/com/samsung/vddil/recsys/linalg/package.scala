package com.samsung.vddil.recsys

/**
 * The linalg contains algorithms and data structures for linear algebra. The 
 * data structures bridge the gap between Scala Breeze and Spark MLLib.  
 */
package object linalg {
    /**
     * This is the precision for zero. Any values whose absolute values are less than 
     * this precision will be treated as zero. This may be used to prevent errors 
     * when normalizing the vector, or dividing.
     */
	val zeroPrecision:Double = 1e-13
}