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

import java.util
import java.util.List

import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.statsd.StrictStatsD
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._
import collection.JavaConversions._

/**
 * Created by pmahendra on 9/2/14.
 */

class IncrementalBackup(config: ServiceConfig, servicePersistence: LocalDB) extends BackupBase(config, servicePersistence) with StrictStatsD {
  override protected val backupType = BackupType.SST

  def execute(keySpaceGiven: String, isCompressed: Boolean): String = {
    val snapshotName = getBackupSnapshotNameForBackupType()
    val keySpacesToBackup:List[String] = keySpaceGiven match {
      case "*" => {
        getKeyspaces()
      }
      case _ => {
        val l:List[String] = new util.ArrayList()
        l.add(keySpaceGiven)
        l
      }
    }

    for(keySpace <- keySpacesToBackup ) {
      executionTime("backup.incremental.elapsed_seconds", s"keyspace:$keySpace", s"compressed:$isCompressed") {
        logger.info(s"keyspace: $keySpaceGiven SST backup requested")
        uploadSst(keySpace, snapshotName, isCompressed)
      }
    }

    getBackupState()
  }

  def uploadSst(keySpace: String, snapshotName: String, isCompressed: Boolean): Long = {
    val backupFmt = if (isCompressed) BackupFormat.Tgz else BackupFormat.Raw

    val sstDirList = getKeySpaceSstDirectoryList(keySpace)

    if( sstDirList.size == 0 ) {
      logger.info(s"No sst backup files found for keyspace: ${keySpace}")
      0
    } else {
      backupTxn(keySpace, snapshotName, backupFmt) {
        var backupResults:BackupResults = new BackupResults()
        sstDirList.foreach(backupDir => {
          uploadDirectory(keySpace = keySpace, snapShotName = snapshotName, backupDir = backupDir, isCompressed = isCompressed) match {
            case r => {
              backupResults = backupResults +  r
            }
          }
        })

        logger.info(s"backuped ${backupResults.filesCount} files")
        backupResults
      }
    }
  }
}

object IncrementalBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new IncrementalBackup(config, servicePersistence)
  }
}