package com.samsung.vddil.recsys.utils

/**
 * Singleton instance of `org.apache.log4j.Logger` used to log activities related
 * to the recommendation engine 
 */
object Logger{
	val logger:org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger("com.samsung.vddil.recsys")
	logger.setLevel(org.apache.log4j.Level.ALL)
	
	/** logs a WARN message */
	def warn(message:String):Unit = {
		logger.warn(message)
	}
	
	/** logs a DEBUG message */
	def debug(message:String):Unit = {
	    logger.debug(message)
	}
	
	/** logs a INFO message */
	def info(message:String):Unit = {
	  	logger.info(message)
	}
	
	/** logs a ERROR message */
	def error(message:String):Unit = {
	    logger.error(message)
	}
}