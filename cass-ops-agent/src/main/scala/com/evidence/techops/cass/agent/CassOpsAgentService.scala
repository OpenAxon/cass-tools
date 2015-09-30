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
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.io.File
import org.apache.commons.daemon.{DaemonContext, Daemon}
import com.evidence.techops.cass.agent.config.ServiceConfig
import org.apache.log4j.PropertyConfigurator

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
  def startServerEx(): Unit =
  {
    // alternative way to start ...
    logger.info("[Starting] Cass-Ops-Agent")

    val processor = new CassandraAgent()
    val serviceAddressPort = s"${ServiceGlobal.config.getServiceAddress()}:${ServiceGlobal.config.getServiceAddressPort()}"
    val server = Thrift.serveIface(serviceAddressPort, processor)

    logger.info(s"[Started] Cass-Ops-Agent running @ $serviceAddressPort")
    Await.ready(server)
  }

  def startServer(): Unit =
  {
    val useTls = ServiceGlobal.config.getTlsEnabled();
    logger.info("[Starting] use tls: " + useTls)

    val processor = new CassandraAgent()
    val service = new CassOpsAgent.FinagledService(processor, new TBinaryProtocol.Factory())
    val certificatePath = ServiceGlobal.config.getTlsCertificatePath()
    val keyPath = ServiceGlobal.config.getTlsCertificateKeyPath()

    val serviceAddressPort = s"${ServiceGlobal.config.getServiceAddress()}:${ServiceGlobal.config.getServiceAddressPort()}"

    logger.info(s"[Started] Cass-Ops-Agent running @ $serviceAddressPort")

    val serviceAddress = new InetSocketAddress(ServiceGlobal.config.getServiceAddress(), ServiceGlobal.config.getServiceAddressPort())

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
    parseArgs(args)

    val configFile = System.getProperty("config.file")

    logger.info(s"[Init] Cass-Ops-Agent")
    logger.info(s"\tconf: ${System.getProperty("config.file")}")
    logger.info(s"\tlogconf: ${System.getProperty("logback.configurationFile")}")

    ServiceGlobal.init(configFile)
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

  private def parseArgs(args: Array[String]):Map[String, String] =
  {
    var rv:Map[String, String] = Map()

    if( args != null && args.length % 2 == 0 )
    {
      for(i <- 0 until args.length if i % 2 == 0 ) {
        rv += (args(i) -> args(i + 1))
      }
    }

    if( !rv.keys.exists(_ == "-logconf") ) {
      val logFile = new File("conf/logback.xml")
      val absPath = logFile.getAbsolutePath()
      rv += ("-logconf" -> absPath)
    }

    // defaults ...
    if(Option(System.getProperty("logback.configurationFile")).getOrElse("") == "") {
      System.setProperty("logback.configurationFile", "file://" + rv("-logconf"))
    }

    if(Option(System.getProperty("config.file")).getOrElse("") == "") {
      val fileTest = new File("conf/cass-ops-agent.conf")
      if( fileTest.exists() )
        rv += ("-conf" -> "conf/cass-ops-agent.conf")
      else
        rv += ("-conf" -> "cass-ops-agent.conf")

      System.setProperty("config.file", rv("-conf"))
    }

    rv
  }
}
