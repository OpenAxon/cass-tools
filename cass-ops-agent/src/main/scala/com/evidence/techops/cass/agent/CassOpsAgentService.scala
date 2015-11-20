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

import com.twitter.finagle.{ListeningServer, Thrift}
import com.twitter.util.Await
import com.evidence.techops.cass.CassOpsAgent
import org.apache.thrift.protocol.TBinaryProtocol
import java.net.InetSocketAddress
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.thrift.ThriftServerBufferedCodec
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.daemon.{DaemonContext, Daemon}
import com.evidence.techops.cass.agent.config.ServiceConfig

/**
 * Created by pmahendra on 9/2/14.
 */

case class TlsSettings(tlsCert: String, tlsCertKey: String)

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
  val serviceConfig = ServiceConfig.load()

  def startServerEx(): Unit =
  {
    // alternative way to start ...
    logger.info("[Starting] Cass-Ops-Agent")

    val processor = new CassandraAgent()
    val serviceAddressPort = s"${serviceConfig.getServiceAddress()}:${serviceConfig.getServiceAddressPort()}"
    val server = Thrift.serveIface(serviceAddressPort, processor)

    logger.info(s"[Started] Cass-Ops-Agent running @ $serviceAddressPort")
    Await.ready(server)
  }

  def startServer(): Unit =
  {
    val useTls = serviceConfig.getTlsEnabled();
    logger.info("[Starting] use tls: " + useTls)

    val processor = new CassandraAgent()
    val service = new CassOpsAgent.FinagledService(processor, new TBinaryProtocol.Factory())
    val certificatePath = serviceConfig.getTlsCertificatePath()
    val keyPath = serviceConfig.getTlsCertificateKeyPath()

    val serviceAddressPort = s"${serviceConfig.getServiceAddress()}:${serviceConfig.getServiceAddressPort()}"

    logger.info(s"[Started] Cass-Ops-Agent running @ $serviceAddressPort")

    val serviceAddress = new InetSocketAddress(serviceConfig.getServiceAddress(), serviceConfig.getServiceAddressPort())

    logger.info(s"\tlisten: $serviceAddress")

    if( useTls ) {
      ServerBuilder()
        .bindTo(serviceAddress)
        .codec(ThriftServerBufferedCodec())
        .maxConcurrentRequests(5)
        .tls(certificatePath, keyPath)
        .name("Cass-Ops-Agent")
        .logChannelActivity(false)
        .build(service)
    } else {
      ServerBuilder()
        .bindTo(serviceAddress)
        .maxConcurrentRequests(5)
        .codec(ThriftServerBufferedCodec())
        .name("Cass-Ops-Agent")
        .logChannelActivity(false)
        .build(service)
    }
  }

  def initConfig(args: Array[String] = null) = {
    logger.info(s"[Init] Cass-Ops-Agent")
    ServiceGlobal.init()
  }

  def main(args: Array[String]) {
    try
    {
      initConfig(args)
      startServer()
    } catch {
      case e: Exception =>
        logger.error(s"Failed starting Cass-Ops-Agent, exiting ${e.toString()}")
        System.exit(1)
    }
  }
}
