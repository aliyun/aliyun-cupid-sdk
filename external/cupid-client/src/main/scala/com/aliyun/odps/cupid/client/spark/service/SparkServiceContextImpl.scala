package com.aliyun.odps.cupid.client.spark.service

import java.io.{File, IOException}
import java.net.URL
import java.util.concurrent.Executors._

import com.aliyun.odps.cupid.client.spark.api.JobContext
import com.aliyun.odps.cupid.client.spark.util.ContextURLClassLoader
import org.apache.log4j.Logger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/**
 * Created by liwei.li on 8/23/16.
 */
class SparkServiceContextImpl(jobContextImpl: JobContextImpl, _jobInfoManager: JobInfoManager, _jarInfoManager: JarInfoManager) extends SparkServiceContext {
  private val logger = Logger.getLogger(this.getClass().getName())
  private val jobThreadNum = (jobContextImpl.sc().getConf.get("spark.driver.cores", "1").toInt * 1.5).toInt

  private val _executionContext = {
    logger.info(s"The SparkService job thread num is $jobThreadNum")
    ExecutionContext.fromExecutorService(newFixedThreadPool(jobThreadNum))
  }

  private val _jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)

  def createJarLocalDir = {
    val localJarDir = new File(SparkServiceConst.JarLocalDir)
    val successFul = localJarDir.mkdir()
    if (successFul) {
      logger.info(s"Create the localjardir successfully")
    } else {
      throw new IOException("Create the localjardir failed")
    }
  }

  createJarLocalDir

  def classLoader(): ContextURLClassLoader = {
    this._jarLoader
  }

  override def getExecutionContext(): ExecutionContextExecutorService = {
    this._executionContext
  }

  override def getJobContext(): JobContext = {
    this.jobContextImpl
  }

  override def getJobInfoManager(): JobInfoManager = {
    this._jobInfoManager
  }

  override def getJarInfoManager(): JarInfoManager = {
    this._jarInfoManager
  }
}
