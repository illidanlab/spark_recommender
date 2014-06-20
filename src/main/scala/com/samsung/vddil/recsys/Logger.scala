package com.samsung.vddil.recsys



object Logger{
	val logger:org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger("com.samsung.vddil.recsys")
	logger.setLevel(org.apache.log4j.Level.ALL)
	
	def warn(message:String):Unit = {
		logger.warn(message)
	}
	
	def debug(message:String):Unit = {
	    logger.debug(message)
	}
	
	def info(message:String):Unit = {
	  	logger.info(message)
	}
	
	def error(message:String):Unit = {
	    logger.error(message)
	}
}