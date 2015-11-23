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
  override protected val backupType = BackupType.SST

  def execute(keySpace: String, isCompressed: Boolean): String = {
    logger.info(s"keyspace: $keySpace backup requested")

    executionTime("backup.incremental.elapsed_seconds", s"keyspace:$keySpace", s"compressed:$isCompressed") {
      uploadSst(keySpace, getBackupSnapshotNameForBackupType(), isCompressed)
      getBackupState()
    }
  }

  def uploadSst(keySpace: String, snapshotName: String, isCompressed: Boolean): Long = {
    var backupFmt = BackupFormat.Raw
    if (isCompressed == true) {
      backupFmt = BackupFormat.Tgz
    }

    val sstDirectoryList = getKeySpaceSstDirectoryList(keySpace) match {
      case None => {
        throw BackupRestoreException(message = Option(s"No sst backup files found for keyspace: ${keySpace}"))
      }
      case Some(snapDirList) => {
        snapDirList
      }
    }

    backupTxn(keySpace, snapshotName, backupFmt) {
      var backupResults:BackupResults = new BackupResults()
      sstDirectoryList.foreach(backupDir => {
        uploadDirectory(keySpace = keySpace, snapShotName = snapshotName, backupDir = backupDir, isCompressed = isCompressed) match {
          case r => {
            backupResults = backupResults +  r
          }
        }
      })

      backupResults
    }
  }
}

object IncrementalBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new IncrementalBackup(config, servicePersistence)
  }
}