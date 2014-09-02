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

package com.evidence.techops.cass.backup

import com.google.common.collect.ImmutableMap
import java.io.File
import com.amazonaws.services.s3.model.ObjectListing
import com.evidence.techops.cass.exceptions.UploadFileException
import com.evidence.techops.cass.agent.ServiceGlobal
import com.evidence.techops.cass.utils.{CassandraNodeProbe, JacksonWrapper}
import com.evidence.techops.cass.backup.BackupType._
import java.util.{TimeZone, Calendar}
import org.joda.time.{Period, DateTime}
import com.evidence.techops.cass.BackupRestoreException
import com.google.gson.Gson
import scala.collection.JavaConversions._
import com.evidence.techops.cass.backup.storage.{Compress, AwsS3}
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.lang.management.MemoryUsage
import org.joda.time.format.DateTimeFormat
import org.apache.commons.io.FileUtils

/**
 * Created by pmahendra on 9/2/14.
 */

case class RemotePath(bucket:String, key:String, fileName:String, keySpace:String, columnFamily:String, localHostId:String)
{
  /* data_directory_location/keyspace_name/table_name */
  def getLocalRestorePathName() = {
    if( Option(ServiceGlobal.config.getRestoreLocalDir()).getOrElse("") != "" ) {
      ServiceGlobal.config.getRestoreLocalDir()
    } else {
      s"${ServiceGlobal.config.getCassDataFileDir()}/${keySpace}/${columnFamily}"
    }
  }
}

abstract class BackupBase extends LazyLogging
{
  protected val FMT = "yyyy-MM-dd-HH:mm:ss"
  protected val FilterKeySpace:List[String] = List("OpsCenter")
  protected val FilterColumnFamily:ImmutableMap[String, List[String]] = ImmutableMap.of("system", List("local", "peers", "LocationInfo"));

  def getClusterStatus():String = {
    try {
      var pingResponse = Map[String, Any]()
      pingResponse += ("cluster" -> getClusterName())
      pingResponse += ("rack" -> getRack())
      pingResponse += ("dc" -> getDataCenter())
      pingResponse += ("host_id" -> getLocalHostId())
      pingResponse += ("host_id_map" -> getHostIdMap())
      pingResponse += ("keyspaces" -> getKeyspaces())
      pingResponse += ("partitioner" -> getPartitioner())
      pingResponse += ("schema_version" -> getSchemaVersion())
      pingResponse += ("uptime" -> getUptime())
      pingResponse += ("current_generation_number" -> getCurrentGenerationNumber())
      pingResponse += ("load_string" -> getLoadString())
      pingResponse += ("operations_mode" -> getOperationMode())

      val heapUsage: MemoryUsage = getHeapMemoryUsage()
      val memUsed: Double = heapUsage.getUsed.asInstanceOf[Double] / (1024 * 1024)
      val memMax: Double = heapUsage.getMax.asInstanceOf[Double] / (1024 * 1024)
      pingResponse += ("heap_usage" -> s"$memUsed/$memMax")
      pingResponse += ("heap_usage_perc" -> (memUsed/memMax) * 100 )

      pingResponse += ("live_nodes" -> getLiveNodes())
      pingResponse += ("unreachable_nodes" -> getUnreachableNodes())
      pingResponse += ("datetime_now" -> DateTime.now().toString(FMT))

      try {
        val snapShotName = ServiceGlobal.database.getState("last_snapshot_name")
        val snapShotStatus = ServiceGlobal.database.getState("last_snapshot_status")
        val filecount = ServiceGlobal.database.getState("last_snapshot_filecount")

        val dateStringFormat = DateTimeFormat.forPattern(FMT);
        val stateDateTime = DateTime.parse(snapShotName, dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime);

        pingResponse += ("last_snap_backup" -> s"$snapShotName/$snapShotStatus")
        pingResponse += ("last_snap_backup_filecount" -> filecount)
        pingResponse += ("last_snap_backup_age_mins" -> diffDuration.toStandardMinutes().getMinutes() )
      } catch {
        case e:Throwable => {
          logger.debug(e.getMessage, e)
          pingResponse += ("last_snap_backup" -> "(null)")
          pingResponse += ("last_snap_backup_filecount" -> -1)
          pingResponse += ("last_snap_backup_age_mins" -> -1 )
        }
      }

      try {
        val state = ServiceGlobal.database.getState("last_sst_name")
        val filecount = ServiceGlobal.database.getState("last_sst_filecount")

        val dateStringFormat = DateTimeFormat.forPattern(FMT);
        val stateDateTime = DateTime.parse(state.split("/")(1), dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime);

        pingResponse += ("last_sst_backup" -> state)
        pingResponse += ("last_sst_backup_filecount" -> filecount)
        pingResponse += ("last_sst_backup_age_mins" -> diffDuration.toStandardMinutes().getMinutes() )
      } catch {
        case e:Throwable => {
          logger.debug(e.getMessage, e)
          pingResponse += ("last_sst_backup" -> "(null)")
          pingResponse += ("last_sst_backup_filecount" -> -1)
          pingResponse += ("last_sst_backup_age_mins" -> -1 )
        }
      }

      try {
        val state = ServiceGlobal.database.getState("last_cl_name")
        val filecount = ServiceGlobal.database.getState("last_cl_filecount")

        val dateStringFormat = DateTimeFormat.forPattern(FMT);
        val stateDateTime = DateTime.parse(state.split("/")(1), dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime);

        pingResponse += ("last_cl_backup" -> state)
        pingResponse += ("last_cl_backup_filecount" -> filecount)
        pingResponse += ("last_cl_backup_age_mins" -> diffDuration.toStandardMinutes().getMinutes() )
      } catch {
        case e:Throwable => {
          logger.debug(e.getMessage, e)
          pingResponse += ("last_cl_backup" -> "(null)")
          pingResponse += ("last_cl_backup_filecount" -> -1)
          pingResponse += ("last_cl_backup_age_mins" -> -1 )
        }
      }

      val response = JacksonWrapper.serialize(pingResponse)
      logger.debug(s"ping response: ${response}")

      response
    } catch {
      case e:Throwable => {
        logger.warn(e.getMessage(), e)
        throw e
      }
    }
  }

  def getColumnFamilyMetric(keySpace:String, colFam:String):String = {
    val func = "getColumnFamilyMetric()"
    try {
      var cfMetricResponse = Map[String, Any]()

      for( cfMetric <- Seq[String]("BloomFilterDiskSpaceUsed",
                                   "BloomFilterFalsePositives",
                                   "BloomFilterFalseRatio",
                                   "CompressionRatio",
                                   "EstimatedColumnCountHistogram",
                                   "EstimatedRowSizeHistogram",
                                   "KeyCacheHitRate",
                                   "LiveSSTableCount",
                                   "MaxRowSize",
                                   "MeanRowSize",
                                   "MemtableColumnsCount",
                                   "MemtableLiveDataSize",
                                   "MinRowSize",
                                   "RecentBloomFilterFalsePositives",
                                   "RecentBloomFilterFalseRatio",
                                   "SnapshotsSize",
                                   "LiveDiskSpaceUsed",
                                   "MemtableSwitchCount",
                                   "SpeculativeRetries",
                                   "TotalDiskSpaceUsed",
                                   "WriteTotalLatency",
                                   "ReadTotalLatency",
                                   "PendingFlushes",
                                   "ReadLatency",
                                   "CoordinatorReadLatency",
                                   "CoordinatorScanLatency",
                                   "WriteLatency",
                                   "LiveScannedHistogram",
                                   "SSTablesPerReadHistogram",
                                   "TombstoneScannedHistogram")
      )
      {
        val metricValue = getColumnFamilyMetric(keySpace, colFam, cfMetric)
        cfMetricResponse += (cfMetric -> metricValue)
      }

      val response = JacksonWrapper.serialize(cfMetricResponse)
      logger.debug(s"ping response: ${response}")

      response
    } catch {
      case e: Throwable => {
        logger.warn(e.getMessage(), e)
        throw e
      }
    }
  }

  protected def uploadDirectory(keySpace:String, columnFamily:String, snapShotName:String, backupType:BackupType.Value, dirToBackup:File, deleteFilesAfterUpload:Boolean, gzipFolderFirst:Boolean):Long = {
    val func = "upload()"

    // assert directory ..
    if( !dirToBackup.exists() || !dirToBackup.isDirectory() ) {
      throw new BackupRestoreException(message = Option(s"Directory to backup ${dirToBackup.getAbsolutePath} invalid!"))
    }

    var statsdBytesMetric = "unknown"
    backupType match {
      case SNAP => {
        statsdBytesMetric = "snapshot"
      }
      case SST => {
        statsdBytesMetric = "incremental"
      }
      case CL => {
        statsdBytesMetric = "commitlog"
      }
      case META => {
        statsdBytesMetric = "metadata"
      }
      case _ => {
        statsdBytesMetric = backupType.toString().toLowerCase()
      }
    }

    val filesTotal = dirToBackup.listFiles().length

    if( gzipFolderFirst )
    {
      val gzipLocalPathName = getGzipLocalPathName(snapShotName, backupType, keySpace, columnFamily)
      val gzipDirectory = new File(gzipLocalPathName)

      logger.debug(s"$func local gzip path: ${gzipDirectory.getAbsolutePath}")
      gzipDirectory.mkdirs()

      // cleanup backup directory ...
      logger.debug(s"$func cleanup local gzip path: ${gzipDirectory.getAbsolutePath}")
      FileUtils.cleanDirectory(gzipDirectory)

      val gzippedFile = new File(s"$gzipLocalPathName/compressed.tar.gz")
      val allFilesInArchive = Compress.createTarGzip(dirToBackup, gzippedFile)

      var filesUploadedToS3 = 0L
      val tarGzFilesToUpload = Array(gzippedFile)
      for(backupFile <- tarGzFilesToUpload)
      {
        val remotePathName = getRemotePathName(snapShotName, backupType, keySpace, columnFamily, backupFile)

        try {
          filesUploadedToS3 += 1
          logger.info(s"$func backing up: ${backupFile.getName} ($filesUploadedToS3 of ${tarGzFilesToUpload.length})")
          uploadFileToRemoteStorage(backupFile, ServiceGlobal.config.getBackupS3BucketName(), remotePathName, statsdBytesMetric)

          if (deleteFilesAfterUpload) {
            if (!backupFile.delete()) {
              throw new BackupRestoreException(message = Option(s"Failed to delete ${backupFile.getAbsolutePath}"))
            }
          }
        } catch {
          case e: Throwable => {
            logger.warn(s"Exception: ${e.getMessage}")
            throw e
          }
        }
      }

      // sanity check ...
      if(filesTotal != allFilesInArchive) {
        logger.error(s"$func files addded to archive ($allFilesInArchive) does not match files found in the directory ($filesTotal)")
      }

      // sanity check ...
      if(filesUploadedToS3 != tarGzFilesToUpload.length) {
        logger.error(s"$func files uploaded to S3 ($filesUploadedToS3) does not match archive files count (${tarGzFilesToUpload.length})")
      }

      allFilesInArchive
    } else {
      var filesUploadedToS3 = 0L
      for(backupFile <- dirToBackup.listFiles())
      {
        val remotePathName = getRemotePathName(snapShotName, backupType, keySpace, columnFamily, backupFile)

        try {
          filesUploadedToS3 += 1
          logger.info(s"$func backing up: ${backupFile.getName} ($filesUploadedToS3 of $filesTotal)")
          uploadFileToRemoteStorage(backupFile, ServiceGlobal.config.getBackupS3BucketName(), remotePathName, statsdBytesMetric)

          if (deleteFilesAfterUpload) {
            if (!backupFile.delete()) {
              throw new BackupRestoreException(message = Option(s"Failed to delete ${backupFile.getAbsolutePath}"))
            }
          }
        } catch {
          case e: Throwable => {
            logger.warn(s"Exception: ${e.getMessage}")
            throw e
          }
        }
      }

      // sanity check ...
      if(filesTotal != filesUploadedToS3) {
        logger.error(s"$func files uploaded to s3 ($filesUploadedToS3) does not match files found in the directory ($filesTotal)")
      }

      filesUploadedToS3
    }
  }

  private def setLastSnapshotState(snapShotName:String, status:String, fileCount:String) = {
    ServiceGlobal.database.saveState("last_snapshot_name", snapShotName)
    ServiceGlobal.database.saveState("last_snapshot_status", status)
    ServiceGlobal.database.saveState("last_snapshot_filecount", fileCount)
  }

  private def setLastSstState(snapShotName:String, status:String, fileCount:String) = {
    val dateTimeNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
    ServiceGlobal.database.saveState("last_sst_name", s"$snapShotName/$dateTimeNow")
    ServiceGlobal.database.saveState("last_sst_filecount", fileCount)
  }

  protected def uploadBackupState(keySpace:String, columnFamily:String, snapShotName:String, backupType:BackupType.Value, backupStatus:String, backupFormat:String, fileCount:String):Unit =
  {
    val remotePathName = getRemotePathName(snapShotName, META, keySpace, columnFamily, null)
    val gson = new Gson()
    val dtNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime)
    val backupState = new BackupState(snapShotName, backupFormat, backupStatus, dtNow.toString(FMT))

    backupType match {
      case SNAP => {
        uploadTextStringToRemoteStorage(gson.toJson(backupState), ServiceGlobal.config.getBackupS3BucketName(), remotePathName + "/BackupStateSNAP.txt", "text/json")
        setLastSnapshotState(snapShotName, backupStatus, fileCount)
      }
      case SST => {
        uploadTextStringToRemoteStorage(gson.toJson(backupState), ServiceGlobal.config.getBackupS3BucketName(), remotePathName + "/BackupStateSST.txt", "text/json")
        setLastSstState(snapShotName, backupStatus, fileCount)
      }
      case _ => {
        throw new UploadFileException(s"${backupType} not supported!")
      }
    }
  }

  protected def uploadMetaData(keySpace:String, columnFamily:String, snapShotName:String, backupType:BackupType.Value, uploadedFileCount:Long):Unit =
  {
    val remotePathName = getRemotePathName(snapShotName, META, keySpace, columnFamily, null)
    val gson = new Gson()

    backupType match {
      case SNAP => {
        val clusterMetaData = new ClusterMetaData(getClusterName(), getDataCenter(), getRack(), getLocalHostId(), getHostIdMap(), getKeyspaces(), getPartitioner(), getReleaseVersion(), getSchemaVersion(), getTokens())
        uploadTextStringToRemoteStorage(gson.toJson(clusterMetaData), ServiceGlobal.config.getBackupS3BucketName(), remotePathName + "/ClusterMetaData.txt", "text/json")

        val snapshotMetaData = new SnapshotMetaData(uploadedFileCount, null)
        uploadTextStringToRemoteStorage(gson.toJson(snapshotMetaData), ServiceGlobal.config.getBackupS3BucketName(), remotePathName + "/SnapshotMetaData.txt", "text/json")
      }
      case SST => {
        val sSTMetaData = new SSTMetaData(uploadedFileCount, null)
        uploadTextStringToRemoteStorage(gson.toJson(sSTMetaData), ServiceGlobal.config.getBackupS3BucketName(), remotePathName + s"/SSTMetaData-${new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)}.txt", "text/json")
      }
      case _ => {
        throw new UploadFileException(s"${backupType} not supported!")
      }
    }
  }

  /**
   * Format of backup path:
   * BASE/CLUSTER-NAME/NODE-UUID/[SNAPSHOTTIME]/[SST|SNAP|CL|META]/KEYSPACE/COLUMNFAMILY
   */

  def getGzipLocalPathName(snapShotTime:String, backupType:BackupType.Value, keySpace:String, columnFamily:String):String =
  {
    val gzipFolder = ServiceGlobal.config.getBackupLocalDir();
    if( Option(gzipFolder).getOrElse("") == "" ) {
      throw new BackupRestoreException(message = Option(s"backup folder missing"))
    }

    val localHostId = getLocalHostId()
    val clusterName = getClusterName()

    s"$gzipFolder/${clusterName}/${localHostId}/${snapShotTime}/${backupType}/${keySpace}/${columnFamily}"
  }

  /**
   * Format of backup path:
   * BASE/CLUSTER-NAME/NODE-UUID/[SNAPSHOTTIME]/[SST|SNAP|CL|META]/KEYSPACE/COLUMNFAMILY/FILE
   */

  def getRemotePathName(snapShotTime:String, backupType:BackupType.Value, keySpace:String = null, columnFamily:String = null, backupFile:File = null):String =
  {
    val localHostId = getLocalHostId()
    val clusterName = getClusterName()

    var path = s"cass-backups/${clusterName}/${localHostId}/${snapShotTime}/${backupType}"

    if( !Option(keySpace).getOrElse("").isEmpty() ) {
      path = path + "/" + keySpace
    }

    if( !Option(columnFamily).getOrElse("").isEmpty() ) {
      path = path + "/" + columnFamily
    }

    if( backupFile != null ) {
      path = path + "/" + backupFile.getName()
    }

    path
  }

  // host id can be null. if null current node host id will be taken
  def getRemoteBackupFiles(remotePathPartial:String, hostIdIn:String):Seq[RemotePath] = {
    val func = "getRemoteBackupFiles()"
    val list = listRemoteDirectory(ServiceGlobal.config.getBackupS3BucketName(), remotePathPartial)
    var rv = Seq[RemotePath]()
    var hostId = hostIdIn

    if( Option(hostId).getOrElse("") == "" ) {
      hostId = getJmxNodeTool().getLocalHostId()
    } else if( hostId != getJmxNodeTool().getLocalHostId() ) {
      logger.warn("*****************************************************")
      logger.warn("*** Current host id is different from backup host ***")
      logger.warn("*****************************************************")
      logger.warn(s"current host id: ${getJmxNodeTool().getLocalHostId()} host id given: $hostIdIn")
    }

    for( objectSummary <- list.getObjectSummaries() ) {
      val bucket = objectSummary.getBucketName()
      val key = objectSummary.getKey()

      logger.info(s"$func found backup file bucket: ${bucket} key: ${key}")
      val keyParts = key.split('/')

      // sanity check ...
      if( keyParts == null || keyParts.length != 8 || hostId != keyParts(2)) // sanity check
        throw new BackupRestoreException(message = Option(s"backup is invalid. key: ${key} bucket: ${bucket} localHostId: ${hostId}"))

        rv = rv :+ new RemotePath(bucket, key, keyParts(7), keyParts(5), keyParts(6), hostId)
    }

    rv
  }

  protected def isValidBackupDir(keySpaceDir:File, columnFamilyDir:File, backupDir:File):Boolean =
  {
    if (!backupDir.isDirectory() && !backupDir.exists())
      return false

    val keySpaceName = keySpaceDir.getName()
    if (FilterKeySpace.contains(keySpaceName))
      return false

    val columnFamilyName = columnFamilyDir.getName()
    if (FilterColumnFamily.containsKey(keySpaceName) && FilterColumnFamily.get(keySpaceName).contains(columnFamilyName))
      return false

    return true
  }

  protected  def clearSnapshot(snapshotName:String, keySpace:String):Unit = getJmxNodeTool().clearSnapshot(snapshotName, keySpace)

  protected def takeSnapshot(snapshotName:String, keySpace:String):Unit = getJmxNodeTool().takeSnapshot(snapshotName, null, keySpace)

  protected def getClusterName() = getJmxNodeTool().getClusterName()

  protected def getRack() = getJmxNodeTool().getRack()

  protected def getDataCenter() = getJmxNodeTool().getDataCenter()

  protected def getLocalHostId() = getJmxNodeTool().getLocalHostId()

  protected def getHostIdMap() = getJmxNodeTool().getHostIdMap()

  protected def getKeyspaces() = getJmxNodeTool().getKeyspaces()

  protected def getPartitioner() = getJmxNodeTool().getPartitioner()

  protected def getReleaseVersion() = getJmxNodeTool().getReleaseVersion()

  protected def getSchemaVersion() = getJmxNodeTool().getSchemaVersion()

  protected def getUptime() = getJmxNodeTool().getUptime()

  protected def getCurrentGenerationNumber() = getJmxNodeTool().getCurrentGenerationNumber()

  protected def getLoadString() = getJmxNodeTool().getLoadString()

  protected def getOperationMode() = getJmxNodeTool().getOperationMode()

  protected def getHeapMemoryUsage() = getJmxNodeTool().getHeapMemoryUsage()

  protected def getLoadMap() = getJmxNodeTool().getLoadMap()

  protected def getUnreachableNodes() = getJmxNodeTool().getUnreachableNodes()

  protected def getLiveNodes() = getJmxNodeTool().getLiveNodes()

  protected def getTokens() = getJmxNodeTool().getTokens()

  protected def getColumnFamilyMetric(ks:String, cf: String, metricName: String) = getJmxNodeTool().getColumnFamilyMetric(ks, cf, metricName)

  protected  def loadNewSSTables(keySpace:String, columnFamilyName:String) = getJmxNodeTool().loadNewSSTables(keySpace, columnFamilyName)

  protected def forceKeySpaceFlush(keySpace:String):Unit = getJmxNodeTool().forceKeyspaceFlush(keySpace)

  protected def getBackupTimeStamp(backupType:BackupType.Value):String = {
    backupType match {
      case SNAP => {
        new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
      }
      case SST | CL => {
        val lastSnap = ServiceGlobal.database.getState("last_snapshot_name")

        if( Option(lastSnap).getOrElse("").isEmpty() )
          throw new BackupRestoreException(message = Option("Take a snapshot before taking an incremental backup or commitlogs backup"))

        lastSnap
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  protected def downloadRemoteObject(bucket:String, key:String, destinationDirectory:File, progress:S3ProgressListener) = {
    ServiceGlobal.config.getBackupStorageType().toLowerCase() match {
      case "aws_s3" => {
        AwsS3.downloadS3Object(bucket, key, destinationDirectory, progress)
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  private def listRemoteDirectory(bucket:String, key:String):ObjectListing =
  {
    ServiceGlobal.config.getBackupStorageType().toLowerCase() match {
      case "aws_s3" => {
        AwsS3.listS3Directory(bucket, key)
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  private def uploadFileToRemoteStorage(source:File, bucket:String, key:String, statsdBytesMetric:String): Unit = {
    ServiceGlobal.config.getBackupStorageType().toLowerCase() match {
      case "aws_s3" => {
        AwsS3.uploadFileToS3(source, bucket, key, statsdBytesMetric)
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  private def uploadTextStringToRemoteStorage(source:String, bucket:String, key:String, contentType:String): Unit = {
    ServiceGlobal.config.getBackupStorageType().toLowerCase() match {
      case "aws_s3" => {
        AwsS3.uploadTextStringToS3(source, bucket, key, contentType)
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  private def getJmxNodeTool() = {
    logger.debug(s"connecting to ${ServiceGlobal.config.getCassJmxHostname()}:${ServiceGlobal.config.getCassJmxPort()}")
    CassandraNodeProbe.getInstanceOf(ServiceGlobal.config.getCassJmxHostname(), ServiceGlobal.config.getCassJmxPort())
  }
}
