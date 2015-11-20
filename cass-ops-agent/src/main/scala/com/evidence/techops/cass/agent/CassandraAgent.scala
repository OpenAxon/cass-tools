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

package com.evidence.techops.cass.agent

import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.statsd.StrictStatsD
import com.twitter.util.{FuturePool, Future}
import com.evidence.techops.cass.backup._
import com.evidence.techops.cass.restore.RestoreBackup
import com.evidence.techops.cass.restore.SSTableLoader
import com.typesafe.scalalogging.LazyLogging
import com.evidence.techops.cass.CassOpsAgent.FutureIface
import com.evidence.techops.cass.BackupRestoreException
import org.joda.time.DateTimeZone

/**
 * Created by pmahendra on 9/2/14.
 */

class CassandraAgent extends FutureIface with LazyLogging with StrictStatsD
{
  DateTimeZone.setDefault(DateTimeZone.UTC)

  private val unboundedPool     = FuturePool.unboundedPool
  private val serviceConfig     = ServiceConfig.load()
  private val snapshotBackup    = SnapshotBackup.apply(serviceConfig)
  private val restoreBackup     = RestoreBackup.apply(serviceConfig)
  private val clBackup          = CommitLogBackup.apply(serviceConfig)
  private val incrementalBackup = IncrementalBackup.apply(serviceConfig)
  private val cassandraNode     = CassandraNode.apply(serviceConfig)

  def getStatus(): Future[String] = {
    unboundedPool {
      executionTime("cmd.getStatus") {
        try {
          logger.info(s"getStatus() [called]")
          cassandraNode.getClusterStatus()
        } catch {
          case e: Throwable => {
            logger.warn(e.getMessage, e)
            throw e
          }
        }
      }
    }
  }

  def getColumnFamilyMetric(keySpace:String, colFam:String): Future[String] = {
    unboundedPool {
      executionTime("cmd.getColumnFamilyMetric") {
        try {
          logger.info(s"getColumnFamilyMetric() [called]")
          cassandraNode.getColumnFamilyMetric(keySpace, colFam)
        } catch {
          case e: Throwable => {
            logger.warn(e.getMessage, e)
            throw e
          }
        }
      }
    }
  }

  def incrementalBackup(keySpace:String): Future[String] = {
    unboundedPool {
      executionTime("cmd.incrementalBackup") {
        if (sstBackupStateChangeOk(true)) {
          try {
            incrementalBackup.execute(keySpace)
          } finally {
            sstBackupStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another SST backup operation already in progress. Try again ..."))
        }
      }
    }
  }

  def commitLogBackup(): Future[String] = {
    unboundedPool {
      executionTime("cmd.commitLogBackup") {
        if (clBackupStateChangeOk(true)) {
          try {
            clBackup.execute()
          } finally {
            clBackupStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another CL backup operation already in progress. Try again ..."))
        }
      }
    }
  }

  def snapshotBackup(keySpace:String): Future[String] = {
    unboundedPool {
      executionTime("cmd.snapshotBackup") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            snapshotBackup.execute(keySpace, false)
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  def snapshotBackup2(keySpace:String): Future[String] = {
    unboundedPool {
      executionTime("cmd.snapshotBackup2") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            snapshotBackup.execute(keySpace, true)
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  def restoreBackup(keySpace:String, snapShotName:String, hostId:String): Future[Unit] = {
    unboundedPool {
      executionTime("cmd.restoreBackup") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            restoreBackup.execute(keySpace, snapShotName, hostId)
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  def csvToSsTableConv(psvFilePath: String, keySpace:String, colFamily:String, partioner:String): Future[String] = {
    if( snapOrRestoreStateChangeOk(true) ) {
      if (!serviceConfig.getSstableBulkImportEnabled()) {
        throw new BackupRestoreException(message = Option("Bulk sstable import operations disabled!"))
      }

      try {
        SSTableLoader.csvToSsTableConv(psvFilePath, keySpace, colFamily, partioner)
      } finally {
        snapOrRestoreStateChangeOk(false)
      }
    } else {
      throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
    }
  }

  def ssTableImport(ssTableFilePath: String, keySpace:String, colFamily:String): Future[Boolean] = {
    if( snapOrRestoreStateChangeOk(true) ) {
      try {
        if (!serviceConfig.getSstableBulkImportEnabled()) {
          throw new BackupRestoreException(message = Option("Bulk sstable import operations disabled!"))
        }

        SSTableLoader.ssTableImport(ssTableFilePath, keySpace, colFamily)
      } finally {
        snapOrRestoreStateChangeOk(false)
      }
    } else {
      throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
    }
  }

  private def clBackupStateChangeOk(opStart:Boolean):Boolean = {
    CassandraAgent.clBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgent.clBackupOrRestoreInProgress == !opStart ) {
        CassandraAgent.clBackupOrRestoreInProgress = opStart
        return true
      } else {
        return false
      }
    }
  }

  private def sstBackupStateChangeOk(opStart:Boolean):Boolean = {
    CassandraAgent.sstBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgent.sstBackupOrRestoreInProgress == !opStart ) {
        CassandraAgent.sstBackupOrRestoreInProgress = opStart
        return true
      } else {
        return false
      }
    }
  }

  private def snapOrRestoreStateChangeOk(opStart:Boolean):Boolean = {
    CassandraAgent.snapBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgent.snapBackupOrRestoreInProgress == !opStart ) {
        CassandraAgent.snapBackupOrRestoreInProgress = opStart
        return true
      } else {
        return false
      }
    }
  }
}

object CassandraAgent extends LazyLogging {
  private var snapBackupOrRestoreInProgress:Boolean = false
  private val snapBackupOrRestoreInProgressLock:Object = new Object()
  private var sstBackupOrRestoreInProgress:Boolean = false
  private val sstBackupOrRestoreInProgressLock:Object = new Object()
  private var clBackupOrRestoreInProgress:Boolean = false
  private val clBackupOrRestoreInProgressLock:Object = new Object()
}


