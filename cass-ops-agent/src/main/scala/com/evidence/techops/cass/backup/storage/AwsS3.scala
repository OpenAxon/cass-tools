/**
 * Copyright 2015 TASER International, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evidence.techops.cass.backup.storage

import com.amazonaws.services.s3.{S3ClientOptions, AmazonS3Client}
import com.evidence.techops.cass.agent.ServiceGlobal
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.transfer.{TransferManagerConfiguration, TransferManager}
import java.io.{ByteArrayInputStream, File}
import com.amazonaws.services.s3.model._
import com.amazonaws.event.{ProgressEventType, ProgressEvent, ProgressListener}
import com.amazonaws.{ClientConfiguration, AmazonServiceException, AmazonClientException}
import com.evidence.techops.cass.exceptions.UploadFileException
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.amazonaws.services.s3.internal.Constants._
import org.joda.time.DateTime

/**
 * Created by pmahendra on 9/18/14.
 */
object AwsS3 extends LazyLogging {
  def uploadFileToS3(sourceFile: File, bucket: String, key: String, statsdBytesMetric:String): Unit =
  {
    val func = "uploadFileToS3()"

    try {
      val request = new PutObjectRequest(bucket, key, sourceFile)
      val contentLength: Long = sourceFile.length()
      var totalBytesTransferred: Long = 0
      var megaBytesTransferred: Double = 0
      var lastLoggedDt: DateTime = new DateTime()

      request.setGeneralProgressListener(new ProgressListener {
        override def progressChanged(progressEvent: ProgressEvent): Unit = {
          if (progressEvent.getEventType == ProgressEventType.TRANSFER_STARTED_EVENT) {
            logger.info(s"\tfile: ${sourceFile.getName} bytes transferred: 0 mb of ${contentLength / (1024 * 1024)}")
          }
          else if (progressEvent.getEventType == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
            totalBytesTransferred += progressEvent.getBytes
            megaBytesTransferred = totalBytesTransferred / (1024 * 1024)
            val now: DateTime = new DateTime
            if (now.getMillis - lastLoggedDt.getMillis > 2000) {
              lastLoggedDt = new DateTime
              logger.info(s"file: ${sourceFile.getName} [bytes transferred] bytes transferred: ${megaBytesTransferred} mb of ${contentLength / (1024 * 1024)}")
            }
          }
          else if (progressEvent.getEventType() == ProgressEventType.TRANSFER_PART_COMPLETED_EVENT) {
            totalBytesTransferred += progressEvent.getBytes
            megaBytesTransferred = totalBytesTransferred / (1024 * 1024)
            logger.info(s"file: ${sourceFile.getName} [part complete] bytes transferred: ${megaBytesTransferred} mb of ${contentLength / (1024 * 1024)}")
          } else if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
            megaBytesTransferred = contentLength / (1024 * 1024)
            logger.info(s"file: ${sourceFile.getName} [complete] bytes transferred: ${megaBytesTransferred} mb of ${megaBytesTransferred}")

            var statsdBytesToRecord:Long = contentLength
            if( statsdBytesToRecord > 2147483647) {
              while (statsdBytesToRecord > 2147483647) {
                // FIX ME: find/make a statsd.count(...) method below that will accept long values ... PM 01/10/2015
                ServiceGlobal.statsd.count(s"backup.${statsdBytesMetric}.aws_s3.completed_bytes", 2147483647)
                statsdBytesToRecord = statsdBytesToRecord - 2147483647
              }
            }

            ServiceGlobal.statsd.count(s"backup.${statsdBytesMetric}.aws_s3.completed_bytes", statsdBytesToRecord.toInt)
          }
        }
      })

      val tm = getS3TransferManager()
      var uploadComplete = false
      var tryCount = 0
      val tryCountMax = 3

      while (tryCount <= tryCountMax && uploadComplete == false)
        try {
          tryCount += 1
          totalBytesTransferred = 0
          megaBytesTransferred = 0

          logger.info(s"$func [start] source=${sourceFile.getAbsolutePath} --> bucket=$bucket, key=$key, tryCount: $tryCount")
          tm.upload(request).waitForCompletion()
          uploadComplete = true

          logger.info(s"$func [done] source=${sourceFile.getAbsolutePath} --> bucket=$bucket, key=$key, tryCount: $tryCount")
        } catch {
          case e:Throwable => {
            if( tryCount <= tryCountMax ) {
              logger.warn(s"$func [failed] source=${sourceFile.getAbsolutePath} --> bucket=$bucket, key=$key, tryCount: $tryCount ${e.getMessage}")
            } else {
              logger.warn(e.getMessage, e)
              logger.error(s"$func [failed] source=${sourceFile.getAbsolutePath} --> bucket=$bucket, key=$key, tryCount: $tryCount")
              throw e
            }
          }
        }
    }
    catch {
      case e:AmazonClientException => {
        logger.debug(e.getLocalizedMessage(), e)
        throw new UploadFileException("Internal error with S3 client", e)
      }
      case e:AmazonServiceException => {
        logger.debug(e.getLocalizedMessage(), e)
        throw new UploadFileException("Error response from S3", e)
      }
    }
  }

  def uploadTextStringToS3(source:String, bucket:String, key:String, contentType:String): Unit = {
    val func = "uploadTextStringToS3()"

    val bytes = source.getBytes()
    val metaData = new ObjectMetadata() {
      setContentMD5(null)
      setContentLength(bytes.length)
      setContentType(contentType)
    }

    val request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(bytes), metaData)
    var totalBytesTransferred: Long = 0
    var megaBytesTransferred: Double = 0
    var lastLoggedDt: DateTime = new DateTime()
    val contentLength:Long = bytes.length

    request.setGeneralProgressListener(new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        if (progressEvent.getEventType == ProgressEventType.TRANSFER_STARTED_EVENT) {
          logger.info(s"file: ${key} bytes transferred: 0 mb of ${contentLength / (1024 * 1024)}")
        }
        else if (progressEvent.getEventType == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
          totalBytesTransferred += progressEvent.getBytes
          megaBytesTransferred = totalBytesTransferred / (1024 * 1024)
          val now: DateTime = new DateTime
          if (now.getMillis - lastLoggedDt.getMillis > 2000) {
            lastLoggedDt = new DateTime
            logger.info(s"file: ${key} [bytes transferred] bytes transferred: ${megaBytesTransferred} mb of ${contentLength / (1024 * 1024)}")
          }
        }
        else if (progressEvent.getEventType() == ProgressEventType.TRANSFER_PART_COMPLETED_EVENT) {
          totalBytesTransferred += progressEvent.getBytes
          megaBytesTransferred = totalBytesTransferred / (1024 * 1024)
          logger.info(s"file: ${key} [part complete] bytes transferred: ${megaBytesTransferred} mb of ${megaBytesTransferred}")
        } else if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
          megaBytesTransferred = contentLength / (1024 * 1024)
          logger.info(s"file: ${key} [complete] bytes transferred: ${megaBytesTransferred} mb of ${megaBytesTransferred}")

          ServiceGlobal.statsd.histogram("backup.aws_s3.completed_bytes", contentLength)
        }
      }
    })

    logger.info(s"$func [start] source=${source} --> bucket=$bucket, key=$key")
    getS3TransferManager().upload(request).waitForCompletion()
    logger.info(s"$func [done] source=${source} --> bucket=$bucket, key=$key")
  }

  def listS3Directory(bucket:String, key:String):ObjectListing = {
    val delimiter = null
    val marker = null
    val request = new ListObjectsRequest(bucket, key, marker, delimiter, 1000)

    val s3Client = getS3Client()
    val objectListing:ObjectListing = s3Client.listObjects(request)

    objectListing
  }

  def downloadS3Object(bucket:String, key:String, destinationDirectory:File, progress:S3ProgressListener):Unit = {
    val tm = getS3TransferManager()

    var totalBytesTransferred: Long = 0
    var contentLength:Long = 0
    var megaBytesTransferred: Double = 0
    var lastLoggedDt: DateTime = new DateTime()

    val getObjRequest = new GetObjectRequest(bucket, key)
    getObjRequest.setGeneralProgressListener(new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        if( progressEvent.getEventType == ProgressEventType.RESPONSE_CONTENT_LENGTH_EVENT) {
          contentLength = progressEvent.getBytes
        } else if (progressEvent.getEventType == ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT) {
          totalBytesTransferred += progressEvent.getBytes
          megaBytesTransferred = totalBytesTransferred / (1024 * 1024)
          val now: DateTime = new DateTime
          if (now.getMillis - lastLoggedDt.getMillis > 2000) {
            lastLoggedDt = new DateTime
            logger.info(s"\tfile: ${destinationDirectory.getName} [bytes transferred] bytes transferred: ${megaBytesTransferred} mb of ${contentLength / (1024 * 1024)}")
          }
        } else if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
          megaBytesTransferred = contentLength / (1024 * 1024)
          logger.info(s"\tfile: ${destinationDirectory.getName} [complete] bytes transferred: ${megaBytesTransferred} mb of ${megaBytesTransferred}")

          ServiceGlobal.statsd.histogram("backup.aws_s3.restored_bytes", contentLength)
        }
      }
    })

    logger.info(s"[download] source: $key --> dest: ${destinationDirectory.getAbsolutePath}")
    val multipleDownloads = tm.download(getObjRequest, destinationDirectory, progress)
    multipleDownloads.waitForCompletion()
  }

  private def getS3Client():AmazonS3Client = {
    var client:AmazonS3Client = null
    val clientCfg = new ClientConfiguration()
    clientCfg.setConnectionTimeout(ServiceGlobal.config.getBackupS3ConnectionTimeoutMs())
    clientCfg.setSocketTimeout(ServiceGlobal.config.getBackupS3SocketimeoutMs())
    clientCfg.setConnectionTTL(0)

    if( ServiceGlobal.config.getBackupS3KeyId() == "" && ServiceGlobal.config.getBackupS3KeySecret() == "") {
      client = new AmazonS3Client(clientCfg)
    } else {
      client = new AmazonS3Client(new AWSCredentials {
        override def getAWSAccessKeyId: String = ServiceGlobal.config.getBackupS3KeyId()
        override def getAWSSecretKey: String = ServiceGlobal.config.getBackupS3KeySecret()
      },
      clientCfg)
    }

    if( Option(ServiceGlobal.config.getBackupS3ServiceURL()).getOrElse("") != "" ) {
      client.setEndpoint(ServiceGlobal.config.getBackupS3ServiceURL())
    }

    if( ServiceGlobal.config.getBackupS3UsePathStyleAccess() ) {
      client.setS3ClientOptions(new S3ClientOptions() {
        setPathStyleAccess(true)
      })
    }

    client
  }

  private def getS3TransferManager():TransferManager = {
    val s3Client = getS3Client()

    val tm = new TransferManager(s3Client)
    tm.setConfiguration(new TransferManagerConfiguration() {
      setMultipartUploadThreshold(5 * MB)
    })

    tm
  }
}
