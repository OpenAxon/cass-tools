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
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.statsd.StrictStatsD
import com.twitter.util.{FuturePool, Future}
import com.evidence.techops.cass.backup._
import com.evidence.techops.cass.restore.RestoreBackup
import com.typesafe.scalalogging.LazyLogging
import com.evidence.techops.cass.CassOpsAgent.FutureIface
import com.evidence.techops.cass.BackupRestoreException
import org.joda.time.DateTimeZone

import scala.util.{Failure, Success, Try}

/**
 * Created by pmahendra on 9/2/14.
 */

class CassandraAgentImpl(serviceConfig: ServiceConfig, servicePersistence: LocalDB) extends FutureIface with LazyLogging with StrictStatsD
{
  DateTimeZone.setDefault(DateTimeZone.UTC)

  private val unboundedPool     = FuturePool        unboundedPool
  private val snapshotBackup    = SnapshotBackup    apply(serviceConfig, servicePersistence)
  private val restoreBackup     = RestoreBackup     apply(serviceConfig, servicePersistence)
  private val clBackup          = CommitLogBackup   apply(serviceConfig, servicePersistence)
  private val incrementalBackup = IncrementalBackup apply(serviceConfig, servicePersistence)
  private val cassandraNode     = CassandraNode     apply(serviceConfig, servicePersistence)

  def getStatus(): Future[String] = {
    unboundedPool {
      executionTime("cmd.getStatus") {
        Try(cassandraNode.getClusterStatus()) match {
          case Success(status) =>
            status
          case Failure(e) =>
            logger.warn(e.getMessage, e)
            throw e
        }
      }
    }
  }

  def getColumnFamilyMetric(keySpace: String, colFam: String): Future[String] = {
    unboundedPool {
      executionTime("cmd.getColumnFamilyMetric") {
        Try(cassandraNode.getColumnFamilyMetric(keySpace, colFam)) match {
          case Success(status) =>
            status
          case Failure(e) =>
            logger.warn(e.getMessage, e)
            throw e
        }
      }
    }
  }

  def incrementalBackup(keySpace: String): Future[String] = {
    unboundedPool {
      executionTime("cmd.incrementalBackup") {
        if (sstBackupStateChangeOk(true)) {
          try {
            incrementalBackup.execute(keySpace, false)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            sstBackupStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another SST backup operation already in progress. Try again ..."))
        }
      }
    }
  }

  def incrementalBackup2(keySpace: String): Future[String] = {
    unboundedPool {
      executionTime("cmd.incrementalBackup") {
        if (sstBackupStateChangeOk(true)) {
          try {
            incrementalBackup.execute(keySpace, true)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
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
            clBackup.execute(isCompressed = false)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            clBackupStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another CL backup operation already in progress. Try again ..."))
        }
      }
    }
  }

  def commitLogBackup2(): Future[String] = {
    unboundedPool {
      executionTime("cmd.commitLogBackup") {
        if (clBackupStateChangeOk(true)) {
          try {
            clBackup.execute(isCompressed = true)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            clBackupStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another CL backup operation already in progress. Try again ..."))
        }
      }
    }
  }

  def snapshotBackup(keySpace: String): Future[String] = {
    unboundedPool {
      executionTime("cmd.snapshotBackup") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            snapshotBackup.execute(keySpace, false)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  def snapshotBackup2(keySpace: String): Future[String] = {
    unboundedPool {
      executionTime("cmd.snapshotBackup2") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            snapshotBackup.execute(keySpace, true)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  def restoreBackup(keySpace: String, snapShotName: String, hostId: String): Future[Unit] = {
    unboundedPool {
      executionTime("cmd.restoreBackup") {
        if (snapOrRestoreStateChangeOk(true)) {
          try {
            restoreBackup.execute(keySpace, snapShotName, hostId)
          } catch {
            case e: Throwable =>
              logger.warn(e.getMessage, e)
              throw e
          } finally {
            snapOrRestoreStateChangeOk(false)
          }
        } else {
          throw new BackupRestoreException(message = Option("Another Backup/Restore or SSTable import operation already in progress. Try again ..."))
        }
      }
    }
  }

  private def clBackupStateChangeOk(opStart: Boolean): Boolean = {
    CassandraAgentImpl.clBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgentImpl.clBackupOrRestoreInProgress == !opStart ) {
        CassandraAgentImpl.clBackupOrRestoreInProgress = opStart
        true
      } else {
        false
      }
    }
  }

  private def sstBackupStateChangeOk(opStart: Boolean): Boolean = {
    CassandraAgentImpl.sstBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgentImpl.sstBackupOrRestoreInProgress == !opStart ) {
        CassandraAgentImpl.sstBackupOrRestoreInProgress = opStart
        true
      } else {
        false
      }
    }
  }

  private def snapOrRestoreStateChangeOk(opStart: Boolean): Boolean = {
    CassandraAgentImpl.snapBackupOrRestoreInProgressLock.synchronized {
      if( CassandraAgentImpl.snapBackupOrRestoreInProgress == !opStart ) {
        CassandraAgentImpl.snapBackupOrRestoreInProgress = opStart
        true
      } else {
        false
      }
    }
  }
}

object CassandraAgentImpl extends LazyLogging {
  private var snapBackupOrRestoreInProgress: Boolean = false
  private val snapBackupOrRestoreInProgressLock: Object = new Object()
  private var sstBackupOrRestoreInProgress: Boolean = false
  private val sstBackupOrRestoreInProgressLock: Object = new Object()
  private var clBackupOrRestoreInProgress: Boolean = false
  private val clBackupOrRestoreInProgressLock: Object = new Object()
}


