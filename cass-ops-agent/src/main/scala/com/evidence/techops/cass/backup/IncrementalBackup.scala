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
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._

/**
 * Created by pmahendra on 9/2/14.
 */

class IncrementalBackup(config: ServiceConfig, servicePersistence: LocalDB) extends BackupBase(config, servicePersistence) with StrictStatsD {
  def execute(keySpace: String, isCompressed: Boolean): String = {
    logger.info(s"keyspace: $keySpace backup requested")

    executionTime("backup.incremental.elapsed_seconds", s"keyspace:$keySpace", s"compressed:$isCompressed") {
      val lastSnapShotTimeStamp = getBackupTimeStamp(SST)

      uploadSst(keySpace, lastSnapShotTimeStamp, isCompressed)

      getState("last_sst_name")
    }
  }

  def uploadSst(keySpace: String, lastSnapShotTimeStamp: String, isCompressed: Boolean): Long = {
    var backedupCountTot = 0L

    var fmt = BackupFormat.Raw
    if (isCompressed == true) {
      fmt = BackupFormat.Tgz
    }

    val snapDirectoryList = getKeySpaceSstDirectoryList(keySpace) match {
      case None => {
        throw BackupRestoreException(message = Option(s"No sst backup files found for keyspace: ${keySpace}"))
      }
      case Some(snapDirList) => {
        snapDirList
      }
    }

    // set backup global state to "inprogress"
    uploadBackupState(keySpace, "[global]", lastSnapShotTimeStamp, SST, "inprogress", fmt, "-1")

    snapDirectoryList.foreach(snapDir => {
      logger.info(s"\tSnapshot ks: $keySpace cf: ${snapDir.cfName} backup path: ${snapDir.directory.getAbsolutePath}")

      // upload to s3 all files under colFamSnapshotDirCurrent ...
      val backedUpFilesCount = uploadDirectory(keySpace = keySpace, columnFamily = snapDir.cfName, snapShotName = lastSnapShotTimeStamp, backupType = SST, dirToBackup = snapDir.directory, deleteSourceFilesAfterUpload = true, compressed = isCompressed)

      // upload meta data ...
      uploadMetaData(keySpace, snapDir.cfName, lastSnapShotTimeStamp, SST, backedUpFilesCount)

      logger.info(s"\t[done] snapshot ks: $keySpace cf: ${snapDir.cfName} backup path: ${snapDir.directory.getAbsolutePath}")

      backedupCountTot += backedUpFilesCount
    })

    // set backup global state to "complete"
    uploadBackupState(keySpace, "[global]", lastSnapShotTimeStamp, SST, "complete", fmt, backedupCountTot.toString)

    backedupCountTot
  }
}

object IncrementalBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new IncrementalBackup(config, servicePersistence)
  }
}