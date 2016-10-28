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

package com.evidence.techops.cass.restore

import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.statsd.StrictStatsD
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._
import com.evidence.techops.cass.backup.{BackupType, BackupBase}
import java.io.File

import org.apache.commons.lang3.StringUtils

/**
 * Created by pmahendra on 9/2/14.
 */

case class RestoredBackup(backupType: BackupType.Value, keySpace:String, cfName:String, snapShotName:String, hostId:String, localFile:File)

class RestoreBackup(config: ServiceConfig, localDb: LocalDB) extends BackupBase(config, localDb) with StrictStatsD
{
  def restoreSnapBackupFiles(keySpace:String, snapShotName:String, hostId:String): Seq[RestoredBackup] = {
    if(StringUtils.isBlank(keySpace) || StringUtils.isBlank(snapShotName) || StringUtils.isBlank(hostId)) {
      throw new BackupRestoreException(message = Option(s"Missing required params keySpace = $keySpace, snapShotName = $snapShotName, hostId = $hostId"));
    }

    restoreBackupFiles(SNAP, snapShotName, keySpace, hostId)
  }

  def restoreSstBackupFiles(keySpace:String, snapShotName:String, hostId:String): Seq[RestoredBackup] = {
    if(StringUtils.isBlank(keySpace) || StringUtils.isBlank(snapShotName) || StringUtils.isBlank(hostId)) {
      throw new BackupRestoreException(message = Option(s"Missing required params keySpace = $keySpace, snapShotName = $snapShotName, hostId = $hostId"));
    }

    restoreBackupFiles(SST, snapShotName, keySpace, hostId)
  }

  def restoreClBackupFiles(snapShotName:String, hostId:String): Seq[RestoredBackup] = {
    if(StringUtils.isBlank(snapShotName) || StringUtils.isBlank(hostId)) {
      throw new BackupRestoreException(message = Option(s"Missing required params snapShotName = $snapShotName, hostId = $hostId"));
    }

    restoreBackupFiles(CL, snapShotName, keySpace =  null, hostId)
  }

  private def restoreBackupFiles(backupType: BackupType.Value, snapshotName:String, keySpace:String, hostId:String): Seq[RestoredBackup] = {
    var rv: Seq[RestoredBackup] = Seq()

    // list all column families
    val remoteBackupFiles = getRemoteBackupFiles(backupType, snapshotName = snapshotName, keySpace = keySpace, hostId = hostId)

    // verify directory
    var targetDirectoryVerified = false

    // foreach column family ...
    for (remoteBackupFile <- remoteBackupFiles) {
      logger.debug(s"remote path: ${remoteBackupFile}")

      // sanity check 1 ...
      if (remoteBackupFile.keySpace != keySpace) {
        throw new BackupRestoreException(message = Option(s"remote path keyspace value: ${remoteBackupFile.keySpace} is invalid"))
      }

      // sanity check 2 ...
      // make sure we are restoring in to the same cluster ...
      if (remoteBackupFile.localHostId != getLocalHostId()) {
        throw new BackupRestoreException(message = Option(s"localHostId value: ${remoteBackupFile.localHostId} is invalid (expected: ${getLocalHostId()}})"))
      }

      // copy files in to data_directory_location/keyspace_name
      val restorePathName = remoteBackupFile.getLocalRestorePathName()
      val destinationDir = new File(restorePathName)

      // create target directory
      if (!destinationDir.exists()) {
        destinationDir.mkdirs()
      }

      // first time seeing this dir? ... sanity check
      if (!targetDirectoryVerified) {
        if ((new File(s"${restorePathName}")).listFiles().size > 0) {
          throw new BackupRestoreException(message = Option(s"${restorePathName} is not empty to begin restore operation!"))
        }

        targetDirectoryVerified = true
      }

      val localFilePath = s"${restorePathName}/${backupType.toString.toLowerCase}/ks-${remoteBackupFile.keySpace}/cf-${remoteBackupFile.columnFamily}"
      val localFileName = s"${localFilePath}/${remoteBackupFile.fileName}"
      val directoryCreated = new File(localFilePath).mkdirs()
      val localFile = new File(localFileName)

      logger.info(s"restoring [${backupType}]: ${remoteBackupFile.bucket}/${remoteBackupFile.key} -> ${localFileName} ($directoryCreated)")

      downloadRemoteObject(remoteBackupFile.bucket, remoteBackupFile.key, localFile, progress = null)

      rv = rv :+ RestoredBackup(backupType, keySpace, remoteBackupFile.columnFamily, snapshotName, hostId, localFile)
    }

    rv
  }

  // host id can be null. if null current node host id will be taken
  def execute(keySpace:String, snapShotName:String, hostId:String):Unit = {
    logger.info(s"keyspace: $keySpace restore snap: $snapShotName requested for host id: $hostId")

    executionTime("backup.restore.elapsed_seconds", s"keyspace:$keySpace", s"snapshot:$snapShotName", s"hostid:$hostId") {
      for (backupType <- Seq(SNAP, SST)) {
        restoreBackupFiles(backupType, keySpace, snapShotName, hostId)
      }
    }
  }
}

object RestoreBackup {
  def apply(config:ServiceConfig, localDb: LocalDB) = {
    new RestoreBackup(config, localDb)
  }
}
