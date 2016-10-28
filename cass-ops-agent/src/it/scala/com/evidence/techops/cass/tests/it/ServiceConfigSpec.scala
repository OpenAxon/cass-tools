package com.evidence.techops.cass.tests.it

import java.io.File
import java.util.UUID

import com.datastax.driver.core.{BoundStatement, Cluster, Session}
import com.evidence.techops.cass.agent.ServiceGlobal
import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.backup.CommitLogBackup
import com.evidence.techops.cass.backup.storage.Compress
import com.evidence.techops.cass.client.Cassandra
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.restore.{RestoreBackup, RestoredBackup}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ServiceConfigSpec extends FlatSpec with Matchers with LazyLogging with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
   ServiceGlobal.init()
  }

  it should "discover data directories via JMX StorageService MBean" in {
    val serviceConfig = ServiceConfig.load()

    val dataDirs = serviceConfig.getCassDataDirList()
    assert(dataDirs.size > 0)
  }
}
