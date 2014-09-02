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
import org.joda.time.{Period, DateTime}
import java.util.{TimeZone, Calendar}

/**
 * Created by pmahendra on 9/2/14.
 */

object IncrementalBackup extends BackupBase {
  def execute(keySpace:String):Future[String] = {
    Future {
      val dtBegin = new DateTime()
      try {
        val func = "execute()"
        logger.info(s"$func keyspace: $keySpace backup requested")

        val lastSnapShotTimeStamp = getBackupTimeStamp(SST)
        val cassandraDataFileDirPath = s"${ServiceGlobal.config.getCassDataFileDir()}"
        val cassandraKeySpaceDirPath = s"${cassandraDataFileDirPath}/${keySpace}"
        val cassandraKeySpaceDir = new File(s"${cassandraKeySpaceDirPath}")

        if (Option(keySpace).getOrElse("").isEmpty() || !cassandraKeySpaceDir.exists()) {
          throw new BackupRestoreException(message = Option(s"$cassandraKeySpaceDirPath missing"))
        }

        val columnFamilyDirectories = cassandraKeySpaceDir.listFiles()
        var backedupCountTot = 0L

        if (columnFamilyDirectories == null || columnFamilyDirectories.length == 0) {
          throw BackupRestoreException(message = Option(s"No incremental backup files found for keyspace: ${keySpace}"))
        }

        uploadBackupState(keySpace, "[global]", lastSnapShotTimeStamp, SST, "inprogress", "raw", "-1")

        for {
          idx <- 0 to (columnFamilyDirectories.length - 1)
          if columnFamilyDirectories(idx).isFile() == false
        } {
          val colFamDir = columnFamilyDirectories(idx)
          val colFamAllIncrBackupsDir = new File(colFamDir, "backups")

          if (isValidBackupDir(cassandraKeySpaceDir, colFamDir, colFamAllIncrBackupsDir)) {
            // column family name
            val columnFamily = colFamDir.getName()
            // upload to s3 ...
            val backedUpFilesCount = uploadDirectory(keySpace, columnFamily, lastSnapShotTimeStamp, SST, colFamAllIncrBackupsDir, true, false)
            // upload meta data ...
            uploadMetaData(keySpace, columnFamily, lastSnapShotTimeStamp, SST, backedUpFilesCount)

            backedupCountTot += backedUpFilesCount
          }
        }

        logger.info(s"$func keyspace: $keySpace backup completed: ${backedupCountTot}")

        uploadBackupState(keySpace, "[global]", lastSnapShotTimeStamp, SST, "complete", "raw", backedupCountTot.toString)

        if (backedupCountTot == 0) {
          throw BackupRestoreException(message = Option(s"No incremental backup files found for keyspace: ${keySpace}"))
        }

        ServiceGlobal.database.getState("last_sst_name")
      } finally {
        ServiceGlobal.statsd.time("backup.incremental.elapsed_seconds", (new Period(dtBegin, new DateTime())).toStandardDuration().toStandardSeconds().getSeconds() * 1000)
      }
    } // Future
  } // execute
}
