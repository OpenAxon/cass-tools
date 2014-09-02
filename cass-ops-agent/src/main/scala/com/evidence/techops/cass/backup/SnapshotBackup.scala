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

import com.twitter.util.Future
import com.evidence.techops.cass.agent.ServiceGlobal
import java.io.File
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._
import java.util.{Map, List}
import org.apache.commons.io.FileUtils
import org.joda.time.{Period, DateTime}

/**
 * Created by pmahendra on 9/2/14.
 */

case class ClusterMetaData(clusterName:String, dataCenter:String, rack:String, localHostId:String, hostIdMap:Map[String, String], keyspaces: List[String], partitioner:String, releaseVersion:String, schemaVersion:String, tokens:List[String])
case class SnapshotMetaData(filesCount:Long, files:Array[String])
case class SSTMetaData(filesCount:Long, files:Array[String])
case class BackupState(name:String, backupFormat:String, status:String, timestamp:String)

object SnapshotBackup extends BackupBase
{
  def execute(keySpace:String, tarGzBackup:Boolean):Future[String] = {
    Future {
      val dtBegin = new DateTime()
      try {
        val func = "execute()"
        logger.info(s"$func keyspace: $keySpace backup requested")

        val snapShotName = getBackupTimeStamp(SNAP)
        val cassandraDataFileDirPath = s"${ServiceGlobal.config.getCassDataFileDir()}"
        val cassandraKeySpaceDirPath = s"${cassandraDataFileDirPath}/${keySpace}"
        val cassandraKeySpaceDir = new File(s"${cassandraKeySpaceDirPath}")
        var backedupCountTot = 0L

        // update last_snapshot_name soon as SNAP starts so that all subsequent SSTs can be organized underneath snapShotName
        var backupFormat = "raw"
        if( tarGzBackup == true ) {
          backupFormat = "tgz"
        }

        if (Option(keySpace).getOrElse("").isEmpty() || !cassandraKeySpaceDir.exists()) {
          throw new BackupRestoreException(message = Option(s"$cassandraKeySpaceDirPath missing"))
        }

        // create the snapshot on the node
        logger.debug(s"$func clear old snapshots for keySpace: ${keySpace}")
        clearSnapshot(null, keySpace)

        // clear local backup/gzip folder
        val localBackupDir = new File(ServiceGlobal.config.getBackupLocalDir)

        if( localBackupDir.exists() && localBackupDir.isDirectory() ) {
          logger.debug(s"$func clean up local folders for backup: ${localBackupDir.getAbsolutePath}")
          FileUtils.cleanDirectory(localBackupDir)
        }

        logger.info(s"$func keySpace: ${keySpace} snapshot: ${snapShotName} ...")

        uploadBackupState(keySpace, "[global]", snapShotName, SNAP, "inprogress", backupFormat, "-1")
        takeSnapshot(snapShotName, keySpace)

        logger.info(s"$func keySpace: ${keySpace} snapshot: ${snapShotName} [OK]!")

        // list keyspace/column family data directories ...
        val columnFamilyDirectories = cassandraKeySpaceDir.listFiles()

        if (columnFamilyDirectories == null || columnFamilyDirectories.length == 0) {
          throw BackupRestoreException(message = Option(s"No snapshot backup files found for keyspace: ${keySpace}"))
        }

        for {
          idx <- 0 to (columnFamilyDirectories.length - 1)
          if columnFamilyDirectories(idx).isFile() == false
        } {
          val colFamDir = columnFamilyDirectories(idx)
          val colFamAllSnapshotsDir = new File(colFamDir, "snapshots")

          if (isValidBackupDir(cassandraKeySpaceDir, colFamDir, colFamAllSnapshotsDir)) {
            val colFamSnapshotDirCurrent = getValidSnapshotDir(snapShotName, keySpace, colFamAllSnapshotsDir)

            if (colFamSnapshotDirCurrent != null) {
              val columnFamily = colFamDir.getName()

              if (colFamSnapshotDirCurrent != null) {
                // upload to s3 all files under colFamSnapshotDirCurrent ...
                val backedUpFilesCount = uploadDirectory(keySpace, columnFamily, snapShotName, SNAP, colFamSnapshotDirCurrent, false, tarGzBackup)
                // upload meta data ...
                uploadMetaData(keySpace, columnFamily, snapShotName, SNAP, backedUpFilesCount)

                backedupCountTot += backedUpFilesCount
              }
            } else {
              logger.info(s"No snapshot files found for keyspace: ${keySpace} snapshot name: ${snapShotName} in ${colFamDir.getAbsolutePath}")
            }
          } else {
            logger.info(s"Skipping ${colFamDir.getAbsolutePath}")
          }
        }

        uploadBackupState(keySpace, "[global]", snapShotName, SNAP, "complete", backupFormat, backedupCountTot.toString)

        logger.info(s"$func clearSnapshot keyspace: $keySpace snapShotName: $snapShotName")
        clearSnapshot(snapShotName, keySpace)
        logger.info(s"$func clearSnapshot keyspace: $keySpace snapShotName: $snapShotName [done]")

        if (backedupCountTot == 0) {
          throw BackupRestoreException(message = Option(s"No snapshot files found for keyspace: ${keySpace}"))
        }

        logger.info(s"$func keyspace: $keySpace backup completed: ${backedupCountTot}")

        ServiceGlobal.database.getState("last_snapshot_name")
      } finally {
        ServiceGlobal.statsd.time("backup.snapshot.elapsed_seconds", (new Period(dtBegin, new DateTime())).toStandardDuration().toStandardSeconds().getSeconds() * 1000)
      }
    } // Future
  } // execute

  private def getValidSnapshotDir(snapshotName:String, keySpace:String, colFamSnapshotsDir:File):File = {
    val func = "getValidSnapshotDir()"

    val allFiles = colFamSnapshotsDir.listFiles()

    if( allFiles == null ) {
      logger.debug(s"$func keyspace: ${keySpace} no snapshots for: ${colFamSnapshotsDir.getAbsolutePath}")
      return null
    }

    for( idx <- 0 to (allFiles.length - 1 ) ) {
      val file = allFiles(idx)
      if( file.getName() == snapshotName) {
        logger.info(s"$func keyspace: ${keySpace} snapshot found: ${file.getAbsolutePath}")
        return file
      }
    }

    logger.debug(s"$func no snapshots for: ${colFamSnapshotsDir.getAbsolutePath}")
    return null
  }
}
