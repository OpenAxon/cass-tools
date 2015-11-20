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

import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.statsd.StrictStatsD
import java.io.File
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._
import java.util.{Map, List}
import collection.JavaConversions._

/**
 * Created by pmahendra on 9/2/14.
 */

case class ClusterMetaData(clusterName:String, dataCenter:String, rack:String, localHostId:String, hostIdMap:Map[String, String], keyspaces: List[String], partitioner:String, releaseVersion:String, schemaVersion:String, tokens:List[String])

case class SnapshotMetaData(filesCount:Long, files:Array[String])

case class SSTMetaData(filesCount:Long, files:Array[String])

case class BackupState(name:String, backupFormat:String, status:String, timestamp:String)

case class SnapshotDirectory(keySpace: String, cfName: String, directory:File)

case class SstDirectory(keySpace: String, cfName: String, directory:File)

class SnapshotBackup(config:ServiceConfig, servicePersistence: LocalDB) extends BackupBase(config, servicePersistence) with StrictStatsD
{
  def execute(keySpace: String, isCompressed: Boolean): String = {
    logger.info(s" keyspace: $keySpace backup requested")

    executionTime("backup.snapshot.elapsed_seconds", s"keyspace:$keySpace", s"compressed:${isCompressed.toString}") {
      val snapShotName = getBackupTimeStamp(SNAP)

      // clear tmp directory used for zipped up files
      cleanBackupTmpDirectory()

      // clear all old snapshots
      clearSnapshot(null, keySpace)

      // take a new snapshot
      takeSnapshot(snapShotName, keySpace)

      logger.info(s"keySpace: ${keySpace} snapshot: ${snapShotName} [OK]!")

      // upload snapshot folders to storage
      val filesUploadedCount = uploadSnapshots(keySpace, snapShotName, isCompressed)

      // clear snapshot
      clearSnapshot(snapShotName, keySpace)

      logger.info(s"Keyspace: $keySpace backup completed: ${filesUploadedCount}")
      getState("last_snapshot_name")
    }
  }

  def uploadSnapshots(keySpace: String, snapShotName: String, isCompressed: Boolean): Long = {
    var backedupCountTot = 0L

    var fmt = BackupFormat.Raw
    if (isCompressed == true) {
      fmt = BackupFormat.Tgz
    }

    val snapDirectoryList = getKeySpaceSnapshotsDirectoryList(keySpace, snapShotName) match {
      case None => {
        throw BackupRestoreException(message = Option(s"No snapshot backup files found for keyspace: ${keySpace} with snapname $snapShotName"))
      }
      case Some(snapDirList) => {
        snapDirList
      }
    }

    // set backup global state to "inprogress"
    uploadBackupState(keySpace, "[global]", snapShotName, SNAP, "inprogress", fmt, "-1")

    snapDirectoryList.foreach(snapDir => {
      logger.info(s"\tSnapshot ks: $keySpace cf: ${snapDir.cfName} backup path: ${snapDir.directory.getAbsolutePath}")

      // upload to s3 all files under colFamSnapshotDirCurrent ...
      val backedUpFilesCount = uploadDirectory(keySpace = keySpace, columnFamily = snapDir.cfName, snapShotName = snapShotName, backupType = SNAP, dirToBackup = snapDir.directory, deleteSourceFilesAfterUpload = false, compressed = isCompressed)

      // upload meta data ...
      uploadMetaData(keySpace, snapDir.cfName, snapShotName, SNAP, backedUpFilesCount)

      logger.info(s"\t[done] snapshot ks: $keySpace cf: ${snapDir.cfName} backup path: ${snapDir.directory.getAbsolutePath}")

      backedupCountTot += backedUpFilesCount
    })

    // set backup global state to "complete"
    uploadBackupState(keySpace, "[global]", snapShotName, SNAP, "complete", fmt, backedupCountTot.toString)

    backedupCountTot
  }
}

object SnapshotBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new SnapshotBackup(config, servicePersistence)
  }
}
