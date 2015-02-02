package com.samsung.vddil.recsys.linalg

import breeze.linalg.{
    norm => brzNorm, DenseMatrix => BDM, 
    sum => brzSum, Axis, axpy => brzAxpy, 
    Vector=> BV, DenseVector=> BDV}
import breeze.numerics.abs

object ProxFunctions {
    /**
     * Solves the proximal operator associated to the l2,1-norm regularization.
     * 
     * returns argmin_X 0.5 * ||X - D||_F^2 + tau * ||X||_{1,2} 
     * 
     * See 
     *  Yuan, Ming, and Yi Lin. "Model selection and estimation in regression with 
     *  grouped variables." Journal of the Royal Statistical Society: Series B (Statistical 
     *  Methodology) 68.1 (2006): 49-67.
     *  
     */
    def proximalL21(D: BDM[Double], tau: Double): BDM[Double] ={
        ( brzSum(D:*D, Axis._1).  //the sum of the suqares of each row Matlab: (sum(D.^2, 2))
                mapValues{ x=>               // element-wise operations   
                    if (x == 0) x            // protect from divide zero exception. 
                    else math.max(0, 1 - tau/scala.math.sqrt(x)) //soft-threshold based on group value. 
                }  
           * BDM(List.fill(D.cols)(1.0))  //replicate the columns Matlab: (repmat)
        ) :* D //element-wise multiplication. 
    }
    
    /**
     * Computes the function value of l2,1 norm.
     * 
     * @return:   ||X||_{1,2} = sum_i||X^i||_2
     */
    def funcValL21(D: BDM[Double]): Double = {
        brzSum(brzSum(D:*D, Axis._1).mapValues{x=>
            scala.math.sqrt(x)
        })
    }
    
    /**
     * 
     * Solves the proximal operator associated to the l1 and l2,1-norm 
     * mixed regularization.
     * 
     * Decomposition theory on sparse:
     *    Dprox = proxL21(proxL1(D))
     *    
     * See Theorem 1 in: 
     * Jiayu Zhou, Jun Liu, Vaibhav A. Narayan, and Jieping Ye. Modeling Disease 
     * Progression via Fused Sparse Group Lasso. KDD 2012
     * 
     * @return argmin_X 0.5 * ||X - D||_F^2 
     *                           + tauL1 * ||X||_{1} + tauL21 * ||X||_{1,2}   
     */
    def proximalL21L1(D: BDM[Double], tauL1: Double, tauL21: Double): BDM[Double] = {
    	proximalL21(proximalL1(D: BDM[Double], tauL1), tauL21)
    }
    
    /**
     * Computes the function value of l2,1 norm.
     * 
     * @return: labmdaL1 * ||X||_{1} + lambdaL21 * ||X||_{1,2}  
     *          = labmdaL1 * sum_i||X^i||_1 + lambdaL21 * sum_i||X^i||_2 
     */
    def funcValL21L1(D: BDM[Double], 
            labmdaL1:Double, lambdaL21:Double): Double = {
        labmdaL1 * funcValL1(D) + lambdaL21 * funcValL21(D)
    }
    
    /**
     * Solves the proximal operator associated to the l1-norm regularization.
     * 
     * returns argmin 0.5 * ||X - D||_F^2 + tau * ||X||_{1} 
     * 
     */    
    def proximalL1(D: BDM[Double], tau: Double): BDM[Double] ={
        ( D.toDenseVector  //the sum of the suqares of each row Matlab: (sum(D.^2, 2))
                 .mapValues{ x=>               // element-wise operations   
                    if (x == 0) x            // protect from divide zero exception. 
                    else 1 - math.min(tau/abs(x),1) //soft-threshold based on group value. 
                }.toDenseMatrix.reshape(D.rows,D.cols)             
        ) :* D //element-wise multiplication. 
    }    
    
    /**
     * Computes the function value of l1 norm.
     * 
     * returns:   ||X||_{1} = sum_i||X^i||_1
     */
    def funcValL1(D: BDM[Double]): Double = {
        brzSum(brzSum(abs(D), Axis._1))
    }   
}