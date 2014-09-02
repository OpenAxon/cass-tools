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

object CommitLogBackup extends BackupBase {
  def execute():Future[String] = {
    Future {
      val dtBegin = new DateTime()
      try {
        val func = "execute()"
        logger.info(s"$func commit logs backup requested")

        val lastSnapShotTimeStamp = getBackupTimeStamp(CL)
        val dateTimeNow = new DateTime(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime).toString(FMT)
        val cassandraCommitLogsDirPath = s"${ServiceGlobal.config.getCassCommitLogDir()}"
        val cassandraCommitLogsDir = new File(s"${cassandraCommitLogsDirPath}")

        if (!cassandraCommitLogsDir.exists()) {
          throw new BackupRestoreException(message = Option(s"$cassandraCommitLogsDirPath missing"))
        }

        // upload to s3 ...
        val backupFilesCount = uploadDirectory(null, null, lastSnapShotTimeStamp, CL, cassandraCommitLogsDir, false, false)
        logger.info(s"$func commit logs backup completed: ${backupFilesCount}")

        ServiceGlobal.database.saveState("last_cl_name", s"$lastSnapShotTimeStamp/$dateTimeNow")
        ServiceGlobal.database.saveState("last_cl_filecount", backupFilesCount.toString)

        if (backupFilesCount == 0) {
          throw BackupRestoreException(message = Option(s"No commit logs to backup"))
        }

        ServiceGlobal.database.getState("last_cl_name")
      } finally {
        ServiceGlobal.statsd.time("backup.commitlog.elapsed_seconds", (new Period(dtBegin, new DateTime())).toStandardDuration().toStandardSeconds().getSeconds() * 1000)
      }
    } // Future
  } // execute
}
