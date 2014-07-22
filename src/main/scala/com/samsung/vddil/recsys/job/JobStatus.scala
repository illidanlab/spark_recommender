package com.samsung.vddil.recsys.job

/**
 * This is the job status. The job status records if the steps are completed, and 
 * for completed steps, if they are successful or failed. The job status also notifies 
 * the job steps available resources (i.e. what are available features to build classifier).
 * 
 * An important usage of this data structure is to store the resource references that are
 * generated. And therefore this is the place where components interact with each other. 
 * See [[com.samsung.vddil.recsys.job.RecJobStatus]] for a concrete example. 
 * 
 */
trait JobStatus {
	/*
	 * This function defines if the tasks are completed. 
	 */
	def allCompleted():Boolean
	/*
	 * This function gives the status in the log file. 
	 */
	def showStatus():Unit
}