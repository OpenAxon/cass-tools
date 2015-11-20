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

package com.evidence.techops.cass.agent.config

import com.typesafe.config.{ConfigFactory, Config}
import java.io.File
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions

/**
 * Created by pmahendra on 9/2/14.
 */

class ServiceConfig(c:Config) extends LazyLogging
{
  // env config variables

  def getEnvId():String = c.getString("env.env_id")

  def getEnvDeploymentCode():String = c.getString("env.deployment_code")

  def getEnvLocationCode():String = c.getString("env.location_code")

  def getEnvServerType():String = c.getString("env.server_type")

  def getEnvServerCode():String = c.getString("env.server_code")

  def getCassPort():Int = c.getInt("cassandra.port")

  def getCassOverTls():Boolean = c.getBoolean("cassandra.tls")

  def getCassRpcHost():String = c.getString("cassandra.rpc_host")

  def getCassRpcPort():Int = c.getInt("cassandra.rpc_port")

  def getCassUsername():String = c.getString("cassandra.username")

  def getCassPassword():String = c.getString("cassandra.password")

  def getCassConfigFileUrl():String = c.getString("cassandra.config_file_url")

  def getCassRackDcConfigFileUrl():String = c.getString("cassandra.rackdc_config_file_url")

  def getCassVersion():String = c.getString("cassandra.version")

  def getCassDataFileDir():String = c.getString("cassandra.data_file_directories")

  def getCassCommitLogDir():String = c.getString("cassandra.commitlog_directory")

  // agent config variables

  def getBackupExclusions() = c.getBoolean("cass_ops_agent.backup.exclusions_enabled")

  def getBackupKeyspaceCfExclusions():java.util.List[String] = c.getStringList("cass_ops_agent.backup.keyspace_cf_exclusions")

  def getBackupS3SocketimeoutMs():Int = c.getInt("cass_ops_agent.backup.socket_timeout_ms")

  def getBackupS3ConnectionTimeoutMs():Int = c.getInt("cass_ops_agent.backup.connection_timeout_ms")

  def getRestoreLocalDir():String  = c.getString("cass_ops_agent.backup.restore_to_dir")

  def getBackupLocalDir():String  = c.getString("cass_ops_agent.backup.backup_to_dir")

  def getBackupS3ServiceURL():String = c.getString("cass_ops_agent.backup.s3_service_url")

  def getBackupS3UsePathStyleAccess():Boolean = c.getBoolean("cass_ops_agent.backup.s3_path_style_access")

  def getBackupS3BucketName():String = c.getString("cass_ops_agent.backup.s3_bucket_name")

  def getBackupS3KeyId():String = c.getString("cass_ops_agent.backup.s3_key_id")

  def getBackupS3KeySecret():String = c.getString("cass_ops_agent.backup.s3_key_secret")

  def getBackupStorageType():String = c.getString("cass_ops_agent.backup.storage_type")

  def getBackupCompressionAlg():String = c.getString("cass_ops_agent.backup.compression_alg").toLowerCase

  def getSstableWriterMaxRows():Int = c.getInt("cass_ops_agent.sstable_writer_max_rows")

  def getSstableLoaderMaxRateMbps():Int = c.getInt("cass_ops_agent.sstable_loader_max_rate_mbps")

  def getSstableWriterBufferSizeMb():Int = c.getInt("cass_ops_agent.sstable_writer_buffer_size_mb")

  def getSstableBulkImportEnabled():Boolean = c.getBoolean("cass_ops_agent.enable_sstable_bulk_import")

  def getTmpFolder():String = c.getString("cass_ops_agent.tmp_data_folder")

  def getCassJmxHostname():String = c.getString("cass_ops_agent.cass_jmx_hostname")

  def getCassJmxPort():Int = c.getInt("cass_ops_agent.cass_jmx_port")

  def getTlsEnabled():Boolean = c.getBoolean("cass_ops_agent.tls.enabled")

  def getTlsCertificatePath():String = c.getString("cass_ops_agent.tls.cert")

  def getTlsCertificateKeyPath():String = c.getString("cass_ops_agent.tls.cert_key")

  def getServiceAddress():String = c.getString("cass_ops_agent.service_address")

  def getServiceAddressPort():Int = c.getInt("cass_ops_agent.service_address_port")

  def getAgentStateDataFolder():String = c.getString("cass_ops_agent.data_folder")

  def getDebugMode():Boolean = c.getBoolean("cass_ops_agent.debug_mode")

  def statsdEnabled = c.getBoolean("statsd.enabled")

  def statsdHost = c.getString("statsd.host")

  def statsdPort = c.getInt("statsd.port")
}

object ServiceConfig extends LazyLogging
{
  def load():ServiceConfig = {
    var originalConfig = ConfigFactory.load()

    val cf = new File("conf/application.conf")
    if (cf.exists()) {
      val applicationConf = ConfigFactory.parseFile(cf)
      originalConfig = applicationConf.withFallback(originalConfig)
    }

    new ServiceConfig(originalConfig)
  }

  def load(file:File):ServiceConfig = {
    new ServiceConfig(ConfigFactory.parseFile(file))
  }
}
