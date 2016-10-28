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

case class ClMetaData(filesCount:Long, files:Array[String])

case class BackupState(name:String, backupFormat:String, status:String, timestamp:String)

case class SnapshotDirectory(keySpace: String, cfName: String, directory:File) extends BackupDirectory

case class SstDirectory(keySpace: String, cfName: String, directory:File) extends BackupDirectory

case class ClDirectory(keySpace: String, cfName: String, directory:File) extends BackupDirectory

class SnapshotBackup(config:ServiceConfig, servicePersistence: LocalDB) extends BackupBase(config, servicePersistence) with StrictStatsD
{
  override protected val backupType = BackupType.SNAP

  def execute(keySpaceGiven: String, isCompressed: Boolean): String = {
    val snapShotName = getBackupSnapshotNameForBackupType()
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

    for(keySpace <- keySpacesToBackup )
    {
      executionTime("backup.snapshot.elapsed_seconds", s"keyspace:$keySpace", s"compressed:$isCompressed") {
        logger.info(s"Keyspace: $keySpaceGiven SST backup requested (snap name: $snapShotName)")

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
      }
    }

    getBackupState()
  }

  def uploadSnapshots(keySpace: String, snapshotName: String, isCompressed: Boolean): Long = {
    val backupFmt = if (isCompressed) BackupFormat.Tgz else BackupFormat.Raw

    val snapDirList = getKeySpaceSnapshotsDirectoryList(keySpace, snapshotName)
    if( snapDirList.size == 0 ) {
      logger.warn(s"No snapshot backup files found for keyspace: ${keySpace} with snapname $snapshotName")
      0
    } else {
      backupTxn(keySpace, snapshotName, backupFmt) {
        var backupResults:BackupResults = new BackupResults()
        snapDirList.foreach(backupDir => {
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

object SnapshotBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new SnapshotBackup(config, servicePersistence)
  }
}
