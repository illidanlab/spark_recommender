package com.samsung.vddil.recsys

/**
 * Defines a list of recommendation jobs in the pipeline
 * 
 * ===Overview===
 * The main trait is given by [[com.samsung.vddil.recsys.job.Job]]. For each job running 
 * in the recommendation pipeline, there is a corresponding job class extending [[com.samsung.vddil.recsys.job.Job]].
 * 
 * A toy implementation is given by [[com.samsung.vddil.recsys.job.HelloWorldJob]], while the 
 * learning to rank recommendation is defined in [[com.samsung.vddil.recsys.job.RecJob]]. It is 
 * easy to define other jobs. For example, traditional factorization-based recommendation. 
 * 
 */
package object job