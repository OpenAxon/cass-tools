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

import com.twitter.util.Future
import com.evidence.techops.cass.agent.ServiceGlobal
import java.io.File
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.backup.BackupType._
import scala.collection.mutable.Set
import com.evidence.techops.cass.backup.{BackupType, BackupBase}
import org.joda.time.{DateTime, Period}

/**
 * Created by pmahendra on 9/2/14.
 */

object RestoreBackup extends BackupBase
{
  // host id can be null. if null current node host id will be taken
  def execute(keySpace:String, snapShotName:String, hostId:String):Future[Unit] = {
    Future {
      val dtBegin = new DateTime()
      try {
        val func = "execute()"
        logger.info(s"$func keyspace: $keySpace restore snap: $snapShotName requested for host id: $hostId")

        val cassandraDataFileDirPath = s"${ServiceGlobal.config.getCassDataFileDir()}"
        val cassandraKeySpaceDirPath = s"${cassandraDataFileDirPath}/${keySpace}"
        val cassandraKeySpaceDir = new File(s"${cassandraKeySpaceDirPath}")

        if (Option(keySpace).getOrElse("").isEmpty() || !cassandraKeySpaceDir.exists()) {
          throw new BackupRestoreException(message = Option(s"$cassandraKeySpaceDirPath missing"))
        }

        var columnFamiliesToRestore = Set[String]()

        for (backupType <- Seq(SNAP, SST)) {
          // get remote snapshot path name ...
          val remotePathPartial = getRemotePathName(snapShotName, backupType, keySpace)

          // list all column families
          val remoteBackupFiles = getRemoteBackupFiles(remotePathPartial, hostId)

          // verify directory
          var targetDirectoryVerified = false

          // foreach column family ...
          for (remoteBackupFile <- remoteBackupFiles) {
            // sanity check 1 ...
            if (remoteBackupFile.keySpace != keySpace)
              throw new BackupRestoreException(message = Option(s"remote path keyspace value: ${remoteBackupFile.keySpace} is invalid"))

            // sanity check 2 ...
            if (Option(remoteBackupFile.columnFamily).getOrElse("").isEmpty())
              throw new BackupRestoreException(message = Option(s"remote path columnFamily value: ${remoteBackupFile.columnFamily} is invalid"))

            // sanity check 3 ...
            // make sure we are restoring in to the same cluster ...
            if (remoteBackupFile.localHostId != getLocalHostId())
              throw new BackupRestoreException(message = Option(s"localHostId value: ${remoteBackupFile.localHostId} is invalid (expected: ${getLocalHostId()}})"))

            // copy files in to data_directory_location/keyspace_name
            val restorePathName = remoteBackupFile.getLocalRestorePathName()

            val destinationDir = new File(restorePathName)

            if (!destinationDir.exists()) {
              throw new BackupRestoreException(message = Option(s"restorePathName: ${restorePathName} doesn't exist"))
            } else {
              // first time seeing this dir? ... sanity check
              if (!targetDirectoryVerified) {
                if ((new File(s"${restorePathName}")).listFiles().size > 0) {
                  throw new BackupRestoreException(message = Option(s"${restorePathName} is not empty to begin restore operation!"))
                }

                targetDirectoryVerified = true
              }
            }

            val localFilePath = s"${restorePathName}/${remoteBackupFile.keySpace}/${remoteBackupFile.columnFamily}"
            val localFileName = s"${localFilePath}/${remoteBackupFile.fileName}"
            val directoryCreated = new File(localFilePath).mkdirs()

            logger.info(s"$func restoring [${backupType}]: ${remoteBackupFile.bucket}/${remoteBackupFile.key} -> ${localFileName} ($directoryCreated)")
            downloadRemoteObject(remoteBackupFile.bucket, remoteBackupFile.key, new File(s"$localFileName"), null)
            columnFamiliesToRestore += remoteBackupFile.columnFamily
          }
        }

        if (columnFamiliesToRestore.size == 0) {
          throw new BackupRestoreException(message = Option(s"${columnFamiliesToRestore.size} files found to restore!"))
        }

        // load sstables ...
        //for (columnFamily <- columnFamiliesToRestore) {
        //  logger.info(s"$func loadNewSSTables keyspace: ${keySpace} column family: ${columnFamily}")
        //  loadNewSSTables(keySpace, columnFamily)
        //}
      } finally {
        ServiceGlobal.statsd.time("backup.restore.elapsed_seconds", (new Period(dtBegin, new DateTime())).toStandardDuration().toStandardSeconds().getSeconds() * 1000)
      }
    } // Future
  } // execute
}
