package com.evidence.techops.cass.tests.it

import java.io.File
import java.util.UUID

import com.datastax.driver.core.{BoundStatement, Session, Cluster}
import com.evidence.techops.cass.agent.ServiceGlobal
import com.evidence.techops.cass.agent.config.ServiceConfig
import com.evidence.techops.cass.backup.storage.Compress
import com.evidence.techops.cass.backup.{BackupBase, SnapshotBackup}
import com.evidence.techops.cass.client.Cassandra
import com.evidence.techops.cass.persistence.LocalDB
import com.evidence.techops.cass.restore.{RestoredBackup, RestoreBackup}
import com.evidence.techops.cass.backup.BackupType._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

/**
 * Created by pmahendra on 11/20/15.
 */

class SnapBackupSpec extends FlatSpec with Matchers with LazyLogging with BeforeAndAfterAll {
  var serviceConfig: ServiceConfig = null
  var cassandraCluster: Cluster = null
  var cassandraSession: Session = null
  var snapTool: SnapshotBackup = null
  var restoreTool: RestoreBackup = null
  var servicePersistence: LocalDB = null

  val testKeyspaceName = "cassops_it_tests"
  val createTestKeysapceIfNotExists = s"CREATE KEYSPACE IF NOT EXISTS $testKeyspaceName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"
  val createTestCf =
    s"""
      |CREATE TABLE IF NOT EXISTS $testKeyspaceName.test_cf (
      |   test_key text,
      |   primary key (test_key)
      |);
    """.stripMargin

  val insertTestData = s"INSERT INTO $testKeyspaceName.test_cf (test_key) VALUES (?)"
  val selectTestData = s"SELECT * FROM $testKeyspaceName.test_cf WHERE test_key = ?"

  override def beforeAll(): Unit = {
    ServiceGlobal.init()

    serviceConfig = ServiceConfig.load()
    cassandraCluster = Cassandra.connect(serviceConfig, false)
    cassandraSession = cassandraCluster.connect()
    servicePersistence = LocalDB.apply(serviceConfig, "it-test-service-persistence")
    snapTool = SnapshotBackup.apply(serviceConfig, servicePersistence)
    restoreTool = RestoreBackup.apply(serviceConfig, servicePersistence)
  }

  it should "create a snapshot backup compressed to s3" in {
    val createTestKeysapceIfNotExistsPrepped = cassandraSession.prepare(createTestKeysapceIfNotExists)
    val createTestCfPrepped = cassandraSession.prepare(createTestCf)

    // create test keyspace
    cassandraSession.execute(new BoundStatement(createTestKeysapceIfNotExistsPrepped))

    // create test cf
    cassandraSession.execute(new BoundStatement(createTestCfPrepped))

    // insert test data
    val insertTestDataPrepped = cassandraSession.prepare(insertTestData)
    val testData = UUID.randomUUID().toString
    cassandraSession.execute(new BoundStatement(insertTestDataPrepped).bind(testData))

    // clear all snapshots
    snapTool.clearAllSnapshots

    // create a new snapshot
    val snapshotName = "it-test-snapshot-" + DateTime.now().toString(ISODateTimeFormat.basicDateTime())
    snapTool.takeSnapshot(snapshotName, testKeyspaceName)

    // verify the keyspace data folder
    val keySpaceDataDir = snapTool.getKeySpaceDataDirectoryList(testKeyspaceName)
    assert(keySpaceDataDir.length > 0)
    keySpaceDataDir.foreach(dir => {
      assert(dir.isDirectory)
    })

    assert(keySpaceDataDir.length == serviceConfig.getCassDataDirList().length)
    logger.info(s"keyspace data dir: ${keySpaceDataDir}")

    // verify the snapshots directory list
    val snapshotsDirList = snapTool.getKeySpaceSnapshotsDirectoryList(testKeyspaceName, snapshotName)

    assert(snapshotsDirList.length > 0)

    // upload snapshot
    val filesBackedupCount = snapTool.uploadSnapshots(testKeyspaceName, snapshotName, isCompressed = true)
    logger.info(s"filesBackedupCount = $filesBackedupCount")
    assert(filesBackedupCount > 0)

    // verify live table
    verifyTestData(testKeyspaceName, testData)

    // download file and verify
    FileUtils.deleteDirectory(new File(serviceConfig.getRestoreLocalDir()))
    val filesRestored = restoreTool.restoreSnapBackupFiles(testKeyspaceName, snapshotName, restoreTool.getLocalHostId)
    assert(filesRestored != null && filesRestored.size > 0)

    // untar files
    val destFolder = new File(serviceConfig.getRestoreLocalDir(), "untarred")
    destFolder.mkdirs()

    filesRestored.foreach((restoredBackup: RestoredBackup) => {
      Compress.extractFiles(restoredBackup.localFile, destFolder)
    })

    val restoredFilesCount = destFolder.listFiles().length
    logger.info(s"restoredFilesCount = $restoredFilesCount filesBackedupCount = $filesBackedupCount")
    assert(restoredFilesCount == filesBackedupCount) // -1 for the backup manifest
  }

  it should "create a snapshot backup to s3" in {
    val createTestKeysapceIfNotExistsPrepped = cassandraSession.prepare(createTestKeysapceIfNotExists)
    val createTestCfPrepped = cassandraSession.prepare(createTestCf)

    // create test keyspace
    cassandraSession.execute(new BoundStatement(createTestKeysapceIfNotExistsPrepped))

    // create test cf
    cassandraSession.execute(new BoundStatement(createTestCfPrepped))

    // insert test data
    val insertTestDataPrepped = cassandraSession.prepare(insertTestData)
    val testData = UUID.randomUUID().toString
    cassandraSession.execute(new BoundStatement(insertTestDataPrepped).bind(testData))

    // clear all snapshots
    snapTool.clearAllSnapshots

    // create a new snapshot
    val snapshotName = "it-test-snapshot-" + DateTime.now().toString(ISODateTimeFormat.basicDateTime())
    snapTool.takeSnapshot(snapshotName, testKeyspaceName)

    // verify the keyspace data folder
    val keySpaceDataDir = snapTool.getKeySpaceDataDirectoryList(testKeyspaceName)
    assert(keySpaceDataDir.length > 0)
    keySpaceDataDir.foreach(dir => {
      assert(dir.isDirectory)
    })

    logger.info(s"keyspace data dir: ${keySpaceDataDir}")

    // verify the snapshots directory list
    val snapshotsDirList = snapTool.getKeySpaceSnapshotsDirectoryList(testKeyspaceName, snapshotName)

    assert(snapshotsDirList.length > 0)

    // upload snapshot
    val filesBackedupCount = snapTool.uploadSnapshots(testKeyspaceName, snapshotName, isCompressed = false)
    logger.info(s"filesBackedupCount = $filesBackedupCount")
    assert(filesBackedupCount > 0)

    // verify live table
    verifyTestData(testKeyspaceName, testData)

    // download file and verify
    FileUtils.deleteDirectory(new File(serviceConfig.getRestoreLocalDir()))
    val filesRestored = restoreTool.restoreSnapBackupFiles(testKeyspaceName, snapshotName, restoreTool.getLocalHostId)
    assert(filesRestored != null && filesRestored.size > 0)

    logger.info(s"filesRestored = ${filesRestored.size} filesBackedupCount = $filesBackedupCount")
    assert(filesRestored.size == filesBackedupCount)
  }

  def verifyTestData(keySpace: String, testData: String) = {
    snapTool.forceKeySpaceFlush(keySpace)
    val selectTestDataPrepped = cassandraSession.prepare(selectTestData)

    val rs = cassandraSession.execute(new BoundStatement(selectTestDataPrepped).bind(testData))
    val allRows = rs.all()

    assert(allRows.size() == 1)
    assert(allRows.get(0).getString("test_key") == testData)
  }
}
