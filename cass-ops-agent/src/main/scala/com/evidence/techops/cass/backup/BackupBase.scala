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

import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.statsd.StrictStatsD
import com.google.common.collect.ImmutableMap
import java.io.File
import com.evidence.techops.cass.exceptions.UploadFileException
import com.evidence.techops.cass.utils.{CassandraNodeProbe, JacksonWrapper}
import com.evidence.techops.cass.backup.BackupType._
import java.util.{TimeZone, Calendar}
import org.apache.cassandra.tools.NodeProbe
import org.apache.commons.lang3.StringUtils
import org.joda.time.{Period, DateTime}
import com.google.gson.Gson
import com.evidence.techops.cass.backup.storage.{AwsS3, Compress}
import com.typesafe.scalalogging.LazyLogging
import java.lang.management.MemoryUsage
import org.joda.time.format.DateTimeFormat
import org.apache.commons.io.FileUtils
import scala.runtime.NonLocalReturnControl
import collection.JavaConversions._
import org.apache.cassandra.utils.UUIDGen

/**
 * Created by pmahendra on 9/2/14.
 */

abstract class BackupDirectory {
  val keySpace: String
  val cfName: String
  val directory:File
}

case class BackupResults(var backupDir:Seq[BackupDirectory], var filesCount:Int) {
  def this() = this(Seq(), 0)

  def +(r:BackupResults):BackupResults = {
    backupDir = backupDir ++ r.backupDir
    filesCount += r.filesCount
    this
  }
}

case class RemotePath(config: ServiceConfig, bucket:String, key:String, fileName:String, keySpace:String, columnFamily:String, localHostId:String) {
  /* data_directory_location/keyspace_name/table_name */
  def getLocalRestorePathName() = {
    if( !Option(config.getRestoreLocalDir()).getOrElse("").isEmpty ) {
      config.getRestoreLocalDir()
    } else {
      throw new BackupRestoreException(message = Option(s"restore_to_dir folder not specified in config!"))
    }
  }
}

trait RemoteBackups
{
  /**
   * Format of backup path:
   * BASE/CLUSTER-NAME/NODE-UUID/[SNAPSHOTNAME]/[SST|SNAP|CL|META]/KEYSPACE/COLUMNFAMILY/FILE
   */

  def getRemotePathName(snapshotName:String, backupType:BackupType.Value, keySpace:String = null, columnFamily:String = null, backupFile:File = null):String = {
    val localHostId = getLocalHostId()
    val clusterName = getClusterName()

    if( backupType == null || StringUtils.isBlank(snapshotName) || StringUtils.isBlank(clusterName) || StringUtils.isBlank(localHostId) ) {
      throw new BackupRestoreException(message = Option(s"Snapshot name, Backup type, and a host id is required"))
    }

    var path = s"cass-backups/${clusterName}/${localHostId}/${snapshotName}/${backupType}"

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

  def getLocalHostId(): String

  def getClusterName(): String
}

class BackupBase(config: ServiceConfig, servicePersistence: LocalDB) extends LazyLogging with RemoteBackups with StrictStatsD
{
  protected val backupType = BackupType.NONE
  protected val FMT = "yyyy-MM-dd-HH:mm:ss"
  protected val FilterKeySpace:List[String] = List("OpsCenter")
  protected val FilterColumnFamily:ImmutableMap[String, List[String]] = ImmutableMap.of("system", List("local", "peers", "LocationInfo"))
  private val backupBlobStoreBucketName = config.getStorageConfig().bucket("cassandra_backups")
  private val blobStorageClient = new AwsS3(config)

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
        val snapShotName = getStateOpt("last_snapshot_name").getOrElse("")
        val snapShotStatus = getStateOpt("last_snapshot_status").getOrElse("")
        val filecount = getStateOpt("last_snapshot_filecount").getOrElse("")

        val dateStringFormat = DateTimeFormat.forPattern(FMT)
        val stateDateTime = DateTime.parse(snapShotName, dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime)

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
        val state = getStateOpt("last_sst_name").getOrElse("")
        val filecount = getStateOpt("last_sst_filecount").getOrElse("")

        val dateStringFormat = DateTimeFormat.forPattern(FMT)
        val stateDateTime = DateTime.parse(state.split("/")(1), dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime)

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
        val state = getStateOpt("last_cl_name").getOrElse("")
        val filecount = getStateOpt("last_cl_filecount").getOrElse("")

        val dateStringFormat = DateTimeFormat.forPattern(FMT)
        val stateDateTime = DateTime.parse(state.split("/")(1), dateStringFormat)
        val diff = new Period(stateDateTime, DateTime.now())
        val diffDuration = diff.toDurationFrom(stateDateTime)

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

      logger.debug(s"ping response: ${pingResponse}")
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

  protected def uploadDirectory(keySpace: String, snapShotName: String, backupDir: BackupDirectory, isCompressed: Boolean):BackupResults = {
    // assert directory ..
    val directory = backupDir.directory
    val columnFamily = backupDir.cfName

    if( !directory.exists() || !directory.isDirectory() ) {
      throw new BackupRestoreException(message = Option(s"Directory to backup ${directory.getAbsolutePath} invalid!"))
    }

    var statsdBytesMetric = "unknown"
    var deleteSourceFilesAfterUpload: Boolean = false

    backupType match {
      case SNAP => {
        statsdBytesMetric = "snapshot"
        deleteSourceFilesAfterUpload = false
      }
      case SST => {
        statsdBytesMetric = "incremental"
        deleteSourceFilesAfterUpload = true
      }
      case CL => {
        statsdBytesMetric = "commitlog"
        deleteSourceFilesAfterUpload = false
      }
      case META => {
        statsdBytesMetric = "metadata"
        deleteSourceFilesAfterUpload = false
      }
      case _ => {
        statsdBytesMetric = backupType.toString().toLowerCase()
        throw new BackupRestoreException(message = Option(s"Unknown backup type ${backupType}"))
      }
    }

    val filesTotalInDirectory = directory.listFiles().length
    val directoryContentsMappedToUpload: Array[File] = isCompressed match {
      case true => {
        val gzipLocalPathName = getGzipLocalPathName(snapShotName, backupType, keySpace, columnFamily)
        val gzipDirectory = new File(gzipLocalPathName)

        logger.debug(s"local gzip path: ${gzipDirectory.getAbsolutePath}")
        gzipDirectory.mkdirs()

        // cleanup backup directory ...
        logger.debug(s"cleanup local gzip path: ${gzipDirectory.getAbsolutePath}")
        FileUtils.cleanDirectory(gzipDirectory)

        val compressedFileId = UUIDGen.getTimeUUID.toString.replace("-", "")          // required so that we don't overwrite previous compressed files under a snapshot name (ex during compressed SST and CL backups)
                                                                                      // Storage option -> Atmos object keys cannot be >= 256 chars in length. So we'll need to keep this as compact as possible.
        val gzippedFile = new File(s"$gzipLocalPathName/compressed-${compressedFileId}.tar.gz")
        val allFilesInArchive = Compress.createTarGzip(directory, gzippedFile)

        // sanity check ...
        if(filesTotalInDirectory != allFilesInArchive) {
          val msg = s"files addded to archive ($allFilesInArchive) does not match files found in the directory ($filesTotalInDirectory)"
          logger.error(msg)
          throw new BackupRestoreException(message = Option(msg))
        }

        Array(gzippedFile)
      }
      case _ => {
        directory.listFiles()
      }
    }

    var filesUploadedToS3 = 0
    for(backupFile <- directoryContentsMappedToUpload)
    {
      val remotePathName = getRemotePathName(snapShotName, backupType, keySpace, columnFamily, backupFile)

      try {
        filesUploadedToS3 += 1
        logger.debug(s"backing up: ${backupFile.getName} ($filesUploadedToS3 of ${directoryContentsMappedToUpload.length})")
        uploadFileToRemoteStorage(backupFile, backupBlobStoreBucketName, remotePathName, statsdBytesMetric)
      } catch {
        case e: Throwable => {
          logger.warn(s"Exception: ${e.getMessage}")
          throw e
        }
      }
    }

    if( isCompressed ) {
      // compressed file is always removed. no reason to leave these around.
      for(backupFile <- directoryContentsMappedToUpload)
      {
        logger.info(s"delete compressed file: ${backupFile.getAbsolutePath}")
        if (!backupFile.delete()) {
          throw new BackupRestoreException(message = Option(s"Failed to delete ${backupFile.getAbsolutePath}"))
        }
      }
    }

    // optionally delete the source file if requested to do so.
    if (deleteSourceFilesAfterUpload) {
      for(sourceFileToBackup <- directory.listFiles()) {
        logger.info(s"delete source file: ${sourceFileToBackup.getAbsolutePath}")
        if (!sourceFileToBackup.delete()) {
          throw new BackupRestoreException(message = Option(s"Failed to delete ${sourceFileToBackup.getAbsolutePath}"))
        }
      }
    }

    // sanity check ...
    if(filesUploadedToS3 != directoryContentsMappedToUpload.length) {
      val msg = s"files uploaded to S3 ($filesUploadedToS3) does not match archive files count (${directoryContentsMappedToUpload.length})"
      logger.error(msg)
      throw new BackupRestoreException(message = Option(msg))
    }

    BackupResults(Seq(backupDir), filesTotalInDirectory)
  }

  private def setLastSnapshotState(snapShotName:String, status:String, fileCount:String) = {
    saveState("last_snapshot_name", snapShotName)
    saveState("last_snapshot_status", status)
    saveState("last_snapshot_filecount", fileCount)
  }

  private def setLastSstState(snapShotName:String, status:String, fileCount:String) = {
    val dateTimeNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
    saveState("last_sst_name", s"$snapShotName/$dateTimeNow")
    saveState("last_sst_filecount", fileCount)
  }

  private def setLastClState(snapShotName:String, status:String, fileCount:String) = {
    val dateTimeNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
    saveState("last_cl_name", s"$snapShotName/$dateTimeNow")
    saveState("last_cl_filecount", fileCount)
  }

  protected def uploadBackupState(keySpace:String, columnFamily:String, snapShotName:String, backupStatus:String, backupFormat:BackupFormat.Value, fileCount:String):Unit = {
    val remotePathName = getRemotePathName(snapShotName, META, keySpace, columnFamily, null)
    val gson = new Gson()
    val dtNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime)
    val backupState = new BackupState(snapShotName, backupFormat.toString.toLowerCase, backupStatus, dtNow.toString(FMT))

    backupType match {
      case SNAP => {
        uploadTextStringToRemoteStorage(gson.toJson(backupState), backupBlobStoreBucketName, remotePathName + "/BackupStateSNAP.txt", "text/json")
        setLastSnapshotState(snapShotName, backupStatus, fileCount)
      }
      case SST => {
        uploadTextStringToRemoteStorage(gson.toJson(backupState), backupBlobStoreBucketName, remotePathName + "/BackupStateSST.txt", "text/json")
        setLastSstState(snapShotName, backupStatus, fileCount)
      }
      case CL => {
        uploadTextStringToRemoteStorage(gson.toJson(backupState), backupBlobStoreBucketName, remotePathName + "/BackupStateCL.txt", "text/json")
        setLastClState(snapShotName, backupStatus, fileCount)
      }
      case _ => {
        throw new UploadFileException(s"${backupType} not supported!")
      }
    }
  }

  protected def uploadMetaData(keySpace: String, columnFamily: String, snapShotName: String, backupResults: BackupResults):Unit = {
    val remotePathName = getRemotePathName(snapShotName, META, keySpace, columnFamily, null)
    val gson = new Gson()

    backupType match {
      case SNAP => {
        val clusterMetaData = new ClusterMetaData(getClusterName(), getDataCenter(), getRack(), getLocalHostId(), getHostIdMap(), getKeyspaces(), getPartitioner(), getReleaseVersion(), getSchemaVersion(), getTokens())
        uploadTextStringToRemoteStorage(gson.toJson(clusterMetaData), backupBlobStoreBucketName, remotePathName + "/ClusterMetaData.txt", "text/json")

        val allBackupedUpFiles:Array[String] = backupResults.backupDir.map(d => d.directory.getAbsolutePath).toArray
        val snapshotMetaData = new SnapshotMetaData(backupResults.filesCount, allBackupedUpFiles)

        uploadTextStringToRemoteStorage(gson.toJson(snapshotMetaData), backupBlobStoreBucketName, remotePathName + "/SnapshotMetaData.txt", "text/json")
      }
      case SST => {
        val allBackupedUpFiles:Array[String] = backupResults.backupDir.map(d => d.directory.getAbsolutePath).toArray
        val sSTMetaData = new SSTMetaData(backupResults.filesCount, allBackupedUpFiles)

        uploadTextStringToRemoteStorage(gson.toJson(sSTMetaData), backupBlobStoreBucketName, remotePathName + s"/SSTMetaData-${new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)}.txt", "text/json")
      }
      case CL => {
        val allBackupedUpFiles:Array[String] = backupResults.backupDir.map(d => d.directory.getAbsolutePath).toArray
        val clMetaData = new ClMetaData(backupResults.filesCount, allBackupedUpFiles)

        uploadTextStringToRemoteStorage(gson.toJson(clMetaData), backupBlobStoreBucketName, remotePathName + s"/SSTMetaData-${new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)}.txt", "text/json")
      }
      case _ => {
        throw new UploadFileException(s"${backupType} not supported!")
      }
    }
  }

  def getKeySpaceDataDirectoryList(keySpace: String): Seq[File] = {
    if (Option(keySpace).getOrElse("").isEmpty() ) {
      throw new BackupRestoreException(message = Option(s"keyspace: $keySpace value cannot be empty"))
    }

    for {
      dataFileDirPath <- config.getCassDataDirList()
      keySpaceDirPath = s"${dataFileDirPath}/${keySpace}"
      keySpaceDir = new File(keySpaceDirPath)
      if keySpaceDir.exists()
    } yield {
      keySpaceDir
    }
  }

  def getClDataDirectory():Option[File] = {
    val cassandraCommitLogsDir = new File(config.getCassCommitLogDir())
    if (!cassandraCommitLogsDir.exists()) {
      None
    } else {
      Some(cassandraCommitLogsDir)
    }
  }

  def getClDirectoryList(): Option[List[ClDirectory]] = {
    val clDirOpt = getClDataDirectory()

    if( !clDirOpt.isDefined ) {
      None
    } else {
      Some(List(ClDirectory(null, null, clDirOpt.get)))
    }
  }

  def getKeySpaceSstDirectoryList(keySpace: String): Seq[SstDirectory] = {
    getKeySpaceDataDirectoryList(keySpace)
      .flatMap(ksDataDir => {
        val sstDirectory = ksDataDir.listFiles()
          .filter(f => f.isDirectory)
          .map(cfDir => SstDirectory(keySpace, cfDir.getName, new File(cfDir, "backups")))
          .filter(snapDir => snapDir.directory.exists())
          .toSeq

        if (sstDirectory == null || sstDirectory.length == 0) {
          None
        } else {
          sstDirectory.foreach(sstFolder => logger.info(s"Keyspace: $keySpace cf: ${sstFolder.cfName} folder found: ${sstFolder.directory.getAbsolutePath}"))
          sstDirectory
        }
      })
  }

  def getKeySpaceSnapshotsDirectoryList(keySpace: String, snapName: String): Seq[SnapshotDirectory] = {
    getKeySpaceDataDirectoryList(keySpace)
      .flatMap(ksDataDir => {
        val snapshotDirectory = ksDataDir.listFiles()
          .filter(f => f.isDirectory)
          .map(cfDir => SnapshotDirectory(keySpace, cfDir.getName, new File(new File(cfDir, "snapshots"), snapName)))
          .filter(snapDir => snapDir.directory.exists())
          .toSeq

        if (snapshotDirectory == null || snapshotDirectory.length == 0) {
          logger.warn(s"No snapshot backup files found for keyspace: $keySpace with snap name: $snapName ks data dir ${ksDataDir.getAbsolutePath}")
          None
        } else {
          snapshotDirectory.foreach(snapFolder => logger.info(s"Keyspace: $keySpace cf: ${snapFolder.cfName} snap name: $snapName snapshot folder found: ${snapFolder.directory.getAbsolutePath}"))
          snapshotDirectory
        }
      })
  }

  def cleanBackupTmpDirectory(): File = {
    val localBackupDir = new File(config.getBackupLocalDir)

    // clear local backup/gzip folder
    if (localBackupDir.exists() && localBackupDir.isDirectory()) {
      logger.debug(s"clean up local folders for backup: ${localBackupDir.getAbsolutePath}")
      FileUtils.cleanDirectory(localBackupDir)
    } else {
      throw new BackupRestoreException(message = Option(s"Backups folder ${config.getBackupLocalDir} missing!"))
    }

    localBackupDir
  }

  /**
   * Format of backup path:
   * BASE/CLUSTER-NAME/NODE-UUID/[SNAPSHOTTIME]/[SST|SNAP|CL|META]/KEYSPACE/COLUMNFAMILY
   */

  def getGzipLocalPathName(snapShotTime:String, backupType:BackupType.Value, keySpace:String, columnFamily:String):String = {
    val gzipFolder = config.getBackupLocalDir()
    if( Option(gzipFolder).getOrElse("") == "" ) {
      throw new BackupRestoreException(message = Option(s"backup folder missing"))
    }

    val localHostId = getLocalHostId()
    val clusterName = getClusterName()

    s"$gzipFolder/${clusterName}/${localHostId}/${snapShotTime}/${backupType}/${keySpace}/${columnFamily}"
  }

  // host id can be null. if null current node host id will be taken
  def getRemoteBackupFiles(backupType : BackupType.Value, snapshotName: String, keySpace: String, hostId: String):Seq[RemotePath] = {
    val remotePathPartial = getRemotePathName(snapshotName, backupType, keySpace)
    val objectSummaryList = listRemoteDirectory(backupBlobStoreBucketName, remotePathPartial)
    var rv = Seq[RemotePath]()
    var hostIdInternal = hostId

    if( Option(hostIdInternal).getOrElse("") == "" ) {
      hostIdInternal = getJmxNodeTool().getLocalHostId()
    } else if( hostIdInternal != getJmxNodeTool().getLocalHostId() )
    {
      logger.warn("*****************************************************")
      logger.warn("*** Current host id is different from backup host ***")
      logger.warn("*****************************************************")
      logger.warn(s"current host id: ${getJmxNodeTool().getLocalHostId()} host id given: $hostId")
    }

    for( objectSummary <- objectSummaryList.getObjectSummaries() ) {
      val bucket = objectSummary.getBucketName()
      val key = objectSummary.getKey()

      logger.info(s"found backup file bucket: ${bucket} key: ${key}")
      val keyParts = key.split('/')

      // sanity check ...
      // key format example: cass-backups/Test Cluster/0cf19477-80b9-4f00-b423-bfcbafa28923/it-test-snapshot-20151123T100224.623-0800/CL/compressed.tar.gz
      //                     sub-folder/cluster-name/node-id/snapshot-name/backup-type/[optional keyspace]/[optional cf name]/file-name

      if( keyParts == null ||
        (keyParts.length != 8 && keyParts.length != 6) ||
        !hostId.equalsIgnoreCase(keyParts(2)) ||
        !backupType.toString.equalsIgnoreCase(keyParts(4)))
      {
        throw new BackupRestoreException(message = Option(s"backup is invalid. key: ${key} bucket: ${bucket} localHostId: ${hostId} backup type: ${backupType.toString}"))
      }

      if( keyParts.length == 8 ) {    // keyspace/cf name present in the key path
        rv = rv :+ new RemotePath(config, bucket = bucket, key = key, fileName = keyParts(7), keySpace = keyParts(5), columnFamily = keyParts(6), localHostId = hostId)
      } else if( keyParts.length == 6 ) { // no keyspace or cf in the key path (ex: commit logs)
        rv = rv :+ new RemotePath(config, bucket = bucket, key = key, fileName = keyParts(5), keySpace = null, columnFamily = null, localHostId = hostId)
      }
    }

    rv
  }

  protected def isValidBackupDir(keySpaceDir:File, columnFamilyDir:File, backupDir:File):Boolean = {
    if (!backupDir.isDirectory() && !backupDir.exists()) {
      false
    } else {
      val keySpaceName = keySpaceDir.getName()

      if (FilterKeySpace.contains(keySpaceName)) {
        false
      } else {
        val columnFamilyName = columnFamilyDir.getName()

        if (FilterColumnFamily.containsKey(keySpaceName) && FilterColumnFamily.get(keySpaceName).contains(columnFamilyName)) {
          false
        } else {
          true
        }
      }
    }
  }

  protected def getBackupState(): String = {
    val s = backupType match {
      case SNAP => {
        "last_snapshot_name"
      }
      case SST => {
        "last_sst_name"
      }
      case CL => {
        "last_cl_name"
      }
      case _ => {
        throw new NotImplementedError()
      }
    }

    servicePersistence.getStateOpt(s).getOrElse("")
  }

  private def getStateOpt(s: String): Option[String] = {
    servicePersistence.getStateOpt(s)
  }

  private def saveState(k: String, v: String) = {
    servicePersistence.saveState(k, v)
  }

  def clearAllSnapshots():Unit = getJmxNodeTool().clearSnapshot(null)

  def clearSnapshot(snapshotName:String, keySpace:String):Unit = {
    logger.debug(s"clear snapshots for keySpace: $keySpace snap name: $snapshotName")
    getJmxNodeTool().clearSnapshot(snapshotName, keySpace)
  }

  def takeSnapshot(snapshotName:String, keySpace:String):Unit = getJmxNodeTool().takeSnapshot(snapshotName, null, keySpace)

  def forceKeyspaceFlush(keySpace:String, cf:String):Unit = getJmxNodeTool().forceKeyspaceFlush(keySpace, cf)

  def forceKeySpaceFlush(keySpace:String):Unit = getJmxNodeTool().forceKeyspaceFlush(keySpace)

  override def getLocalHostId() = getJmxNodeTool().getLocalHostId()

  override def getClusterName() = getJmxNodeTool().getClusterName()

  protected def getRack() = getJmxNodeTool().getRack()

  protected def getDataCenter() = getJmxNodeTool().getDataCenter()

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

  protected def getBackupSnapshotNameForBackupType():String = {
    backupType match {
      case SNAP => {
        new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
      }
      case SST | CL => {
        val lastSnap = getStateOpt("last_snapshot_name").getOrElse("")

        if( Option(lastSnap).getOrElse("").isEmpty() )
          throw new BackupRestoreException(message = Option("Take a snapshot before taking an incremental backup or commitlogs backup"))

        lastSnap
      }
      case _ => {
        throw new NotImplementedError()
      }
    }
  }

  protected def downloadRemoteObject(bucket: String, key: String, destinationDirectory: File, progress: S3ProgressListener) = {
    blobStorageClient.downloadS3Object(bucket = bucket, key = key, destinationDirectory = destinationDirectory, progress = progress)
  }

  private def listRemoteDirectory(bucket: String, key: String): ObjectListing = {
    blobStorageClient.listS3Directory(bucket = bucket, prefix = key)
  }

  private def uploadFileToRemoteStorage(source: File, bucket: String, key: String, statsdBytesMetric: String): Unit = {
    logger.info(s"[start] upload file bytes: ${source.length()} source: $source bucket: $bucket key :$key")
    blobStorageClient.uploadFileToS3(sourceFile = source, bucket = bucket, key = key, statsdBytesMetric = statsdBytesMetric)
    logger.info(s"[done ] upload file bytes: ${source.length()} source: $source bucket: $bucket key :$key")

    statsd.count(s"backup.${statsdBytesMetric}.${config.getStorageConfig().provider()}.completed_bytes", source.length())
  }

  private def uploadTextStringToRemoteStorage(source: String, bucket: String, key: String, contentType: String): Unit = {
    logger.info(s"[start] upload text len: ${source.length} bucket: $bucket key: $key contentType: $contentType")
    blobStorageClient.uploadTextStringToS3(source = source, contentType = contentType, bucket = bucket, key = key)
    logger.info(s"[done ] upload text len: ${source.length} bucket: $bucket key: $key contentType: $contentType")
  }

  private def getJmxNodeTool(): NodeProbe = {
    logger.debug(s"connecting to ${config.getCassJmxHostname()}:${config.getCassJmxPort()} user: ${config.getCassJmxUsername()}")
    CassandraNodeProbe.instanceOf(config.getCassJmxHostname(), config.getCassJmxPort(), config.getCassJmxUsername(), config.getCassJmxPassword())
  }

  def backupTxn[T](keySpace: String, snapshotName: String, backupFmt: BackupFormat.Value)(thunk: => BackupResults): Int = {

    uploadBackupState(keySpace, "[global]", snapshotName, "inprogress", backupFmt, "-1")

    val backupResults:BackupResults = try {
      thunk
    } catch {
      case nlr: NonLocalReturnControl[BackupResults@unchecked] => nlr.value
    }

    // upload keyspace/cf meta data ...
    backupResults.backupDir.foreach(backupDir => {
      uploadMetaData(keySpace, backupDir.cfName, snapshotName, backupResults)
    })

    uploadBackupState(keySpace, "[global]", snapshotName, "complete", backupFmt, backupResults.filesCount.toString)

    backupResults.filesCount
  }
}
