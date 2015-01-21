package org.apache.spark.mllib.optimization

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import breeze.linalg.{norm => brzNorm, DenseMatrix => BDM, axpy => brzAxpy, Vector=> BV, DenseVector=> BDV}
import breeze.linalg.{sum => brzSum}
import scala.collection.immutable.HashMap
import breeze.linalg.Axis
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

object FactorizationMachineRegressionSparseModel{
    /**
     * Defines sparse gradient update rule. This rule does not penalize the intercept. 
     * 
     * @param latentDim the latent dimension of the rank of interaction matrix
     * @param the parameter for regularization. 
     */
    class FactorizationMachineL21Update (
            val latentDim:Int, l21Param: Double) extends Updater{
        override def compute(
	      weightsOld: Vector,
	      gradient: Vector,
	      stepSize: Double,
	      iter: Int,
	      regParam: Double
	      ):(Vector, Double) = {
            val thisIterStepSize = stepSize / math.sqrt(iter)
            val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
            
            //gradient update 
            // add up both updates from the gradient of the loss (= step) as well as
		    // the gradient of the regularizer (= regParam * weightsOld)
		    // w' = w - thisIterStepSize * (gradient + regParam * w)
		    //    = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
            
            //Shrinkage (except for intercept)
            // Here we use a small trick: replacing the intercept value back instead 
            // of segmenting.  
            val w0Weights = brzWeights(brzWeights.size - 1) //save the intercept weight
            brzWeights :*= (1.0 - thisIterStepSize * regParam)//shrinkage
            brzWeights(brzWeights.size - 1) = w0Weights //recover the intercept weight
            
            brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
            
            //norm without w0 
            //NOTE: scala breeze 0.7 does not support brzNorm on slice 
            val norm = brzWeights(0 to brzWeights.size - 2).norm(2.0)
            
            //proximal update
    		val (wVector, vMatrix, w0) = FactorizationMachineRegressionModel.devectorize(brzWeights.toDenseVector, latentDim)
    		
    		val proxMat = proximalL21(BDM.horzcat(wVector.toDenseMatrix.t, vMatrix), l21Param/thisIterStepSize)
    		val wVectorProx = proxMat(::, 0)
    		val vMatrixProx = proxMat(::, 1 to proxMat.cols - 1)
    		val brzWeightsProx = FactorizationMachineRegressionModel.vectorize(wVectorProx, vMatrixProx, w0)
            
    		(Vectors.fromBreeze(brzWeightsProx), 0.5 * regParam * norm * norm + l21Param * funcValL21(proxMat))
        }
    }
    
    /**
     * Solves the proximal operator associated to the l2,1-norm regularization.
     * 
     * returns argmin 0.5 * ||X - D||_F^2 + tau * ||X||_{1,2} 
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
     * returns:   ||X||_{1,2} = sum_i||X^i||_2
     */
    def funcValL21(D: BDM[Double]): Double = {
        brzSum(brzSum(D:*D, Axis._1).mapValues{x=>
            scala.math.sqrt(x)
        })
    }
}


class FactorizationMachineRegressionSparseWithSGD private (
		private var latentDim:Int,
		private var stepSize: Double,
		private var numIterations: Int,
		private var regParam: Double,
		private var l21Param: Double,
		private var miniBatchFraction: Double)
	extends CustomizedAlgorithm[FactorizationMachineRegressionModel] with Serializable {
    
    private val gradient:Gradient = 
        new FactorizationMachineRegressionModel.FactorizationMachineGradient(latentDim)
    private val updater:Updater  = 
        new FactorizationMachineRegressionSparseModel.FactorizationMachineL21Update(latentDim, l21Param)
    
    override val optimizer = new GradientDescent(gradient, updater)
        .setStepSize(stepSize)
        .setNumIterations(numIterations)
        .setRegParam(regParam)
        .setMiniBatchFraction(miniBatchFraction)
        
    /**
     * Construct a RidgeRegression object with default parameters: {stepSize: 1.0, numIterations: 100,
     * regParam: 1.0, miniBatchFraction: 1.0}.
     */
    def this(latentDim: Int, l21Param:Double) = this(latentDim, 0.0001, 100, 1.0, l21Param, 1.0)    
    
	override protected def createModel(weights: Vector, param:HashMap[String, Any] ) = {
		new FactorizationMachineRegressionModel(weights, param)
	}
}

/**
 * Top-level methods for calling FactorizationMachineRegression 
 */
object FactorizationMachineRegressionSparseWithSGD{
    def train(
            input: RDD[LabeledPoint],
            latentDim: Int,
            numIterations: Int,
            stepSize: Double,
            regParam: Double,
            l21Param: Double,
            miniBatchFraction: Double,
            initialWeights: Vector): FactorizationMachineRegressionModel = {
        
        new FactorizationMachineRegressionSparseWithSGD(latentDim, stepSize, numIterations, regParam, l21Param, miniBatchFraction).run(
                input, initialWeights, HashMap[String, Any](
                FactorizationMachineRegressionModel.ParamLatentDimension -> latentDim))
    }
    
    def train(
            input: RDD[LabeledPoint],
            latentDim: Int,
            numIterations: Int,
            stepSize: Double,
            regParam: Double,
            l21Param: Double,
            miniBatchFraction: Double): FactorizationMachineRegressionModel = {
        
        val featureDim:Int = input.first.features.size
        val initialWeights: Vector = FactorizationMachineRegressionModel.initUnitModel(featureDim, latentDim)
        
        FactorizationMachineRegressionSparseWithSGD.train(
                input, latentDim, numIterations, stepSize, regParam, l21Param, miniBatchFraction, initialWeights)
        
    }
    
}
