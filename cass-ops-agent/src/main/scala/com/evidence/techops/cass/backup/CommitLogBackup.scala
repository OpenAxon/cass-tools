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
import java.util.{TimeZone, Calendar}

import org.joda.time.DateTime

/**
 * Created by pmahendra on 9/2/14.
 */

class CommitLogBackup(config: ServiceConfig, servicePersistence: LocalDB) extends BackupBase(config, servicePersistence) with StrictStatsD {
  def execute(): String = {
    logger.info(s"commit logs backup requested")

    executionTime("backup.commitlog.elapsed_seconds") {
      val lastSnapShotTimeStamp = getBackupTimeStamp(CL)
      val dateTimeNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
      val cassandraCommitLogsDirPath = s"${config.getCassCommitLogDir()}"
      val cassandraCommitLogsDir = new File(s"${cassandraCommitLogsDirPath}")

      if (!cassandraCommitLogsDir.exists()) {
        throw new BackupRestoreException(message = Option(s"$cassandraCommitLogsDirPath missing"))
      }

      logger.info(s"commit logs backup started. folder: ${cassandraCommitLogsDir.getAbsolutePath}")

      // upload commit logs to storage
      val backupFilesCount = uploadDirectory(keySpace = null, columnFamily = null, snapShotName = lastSnapShotTimeStamp, backupType = CL, dirToBackup = cassandraCommitLogsDir, deleteSourceFilesAfterUpload = false, compressed = false)

      logger.info(s"commit logs backup completed: ${backupFilesCount}")

      saveState("last_cl_name", s"$lastSnapShotTimeStamp/$dateTimeNow")
      saveState("last_cl_filecount", backupFilesCount.toString)

      getState("last_cl_name")
    }
  }
}

object CommitLogBackup {
  def apply(config:ServiceConfig, servicePersistence: LocalDB) = {
    new CommitLogBackup(config, servicePersistence)
  }
}
