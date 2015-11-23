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

import com.evidence.techops.cass.persistence.LocalDB
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.ListeningServer
import com.twitter.util.Await
import java.net.InetSocketAddress
import com.twitter.finagle.ThriftMux
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.daemon.{DaemonContext, Daemon}
import com.evidence.techops.cass.agent.config.ServiceConfig

/**
 * Created by pmahendra on 9/2/14.
 */

class CassOpsAgentService extends Daemon with LazyLogging {

  var config: ServiceConfig = _
  var server: ListeningServer = _

  def init(daemonContext: DaemonContext): Unit = {
    logger.info(s"[CassOpsAgentService] Loading ${System.getProperty("config.file")} ...")
    CassOpsAgentService.initConfig()
  }

  def start(): Unit = {
    logger.info(s"[CassOpsAgentService] Starting ...")
    CassOpsAgentService.startServer()
  }

  def stop(): Unit = {
    logger.info(s"[CassOpsAgentService] Stopping ...")
    server.close()
    logger.info(s"[CassOpsAgentService] Stopped")
  }

  def destroy(): Unit = {
    config = null
  }
}

object CassOpsAgentService extends LazyLogging
{
  def startServer(): Unit =
  {
    val serviceConfig = ServiceConfig.load()
    val servicePersistence = LocalDB.apply(serviceConfig, "cass-ops-agent-db")

    val useTls = serviceConfig.getTlsEnabled();
    logger.info("[Starting] use tls: " + useTls)

    val service = new CassandraAgentImpl(serviceConfig, servicePersistence)
    val certificatePath = serviceConfig.getTlsCertificatePath()
    val keyPath = serviceConfig.getTlsCertificateKeyPath()

    val serviceAddressPort = s"${serviceConfig.getServiceAddress()}:${serviceConfig.getServiceAddressPort()}"
    val serviceAddress = new InetSocketAddress(serviceConfig.getServiceAddress(), serviceConfig.getServiceAddressPort())

    logger.info(s"[Started] Cass-Ops-Agent running @ $serviceAddressPort")
    logger.info(s"\tlisten: $serviceAddress")

    var thriftMuxServer = ThriftMux.server
    val logChannelActivity = false

    thriftMuxServer = thriftMuxServer.configured(Transport.Verbose(logChannelActivity))
    thriftMuxServer = useTls match {
      case true => {
        thriftMuxServer.configured(Transport.TLSServerEngine(Some(() => Ssl.server(certificatePath, keyPath, /* caCertificatePath */ null, /* ciphers */ null, /* nextProtos */ null))))
      }
      case _ => {
        thriftMuxServer
      }
    }

    val server = thriftMuxServer.serveIface(serviceAddress, service)
    Await.ready(server)
  }

  def initConfig(args: Array[String] = null) = {
    logger.info(s"[Init] Cass-Ops-Agent")
    ServiceGlobal.init()
  }

  def main(args: Array[String]): Unit = {
    try {
      initConfig(args)
      startServer()
    } catch {
      case e: Exception =>
        logger.error(s"Failed starting Cass-Ops-Agent, exiting ${e.toString()}")
        System.exit(1)
    }
  }
}
