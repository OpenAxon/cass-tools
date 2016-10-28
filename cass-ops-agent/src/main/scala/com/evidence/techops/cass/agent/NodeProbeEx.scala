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

import java.lang.management.{RuntimeMXBean, MemoryMXBean, ManagementFactory}
import javax.management.{MalformedObjectNameException, MBeanServerConnection, JMX, ObjectName}
import javax.management.remote.{JMXConnectorFactory, JMXConnector, JMXServiceURL}
import com.evidence.techops.cass.agent.config.ServiceConfig
import org.apache.cassandra.service._

class NodeProbeEx(config: ServiceConfig)
{
  private val ssObjName = "org.apache.cassandra.db:type=StorageService"
  private var jmxc: JMXConnector = null
  private var mbeanServerConn: MBeanServerConnection = null
  private var ssProxy: StorageServiceMBean = null
  private var memProxy: MemoryMXBean = null
  private var runtimeProxy: RuntimeMXBean = null

  private val host = config.getCassJmxHostname()
  private val port = config.getCassJmxPort()
  private val username = config.getCassJmxUsername()
  private val password = config.getCassJmxPassword()

  connect()

  def dataDirs() = ssProxy.getAllDataFileLocations()

  private def connect() = {
    val jmxUrl = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$host:$port/jmxrmi")
    val env = new java.util.HashMap[String, AnyRef]

    if (username != null) {
      env.put(JMXConnector.CREDENTIALS, Array(username, password))
    }

    jmxc = JMXConnectorFactory.connect(jmxUrl, env)
    mbeanServerConn = jmxc.getMBeanServerConnection

    try {
      ssProxy = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(ssObjName), classOf[StorageServiceMBean])
    } catch {
      case e: MalformedObjectNameException =>
        throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e)
    }

    memProxy = ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, ManagementFactory.MEMORY_MXBEAN_NAME, classOf[MemoryMXBean])
    runtimeProxy = ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, ManagementFactory.RUNTIME_MXBEAN_NAME, classOf[RuntimeMXBean])
  }
}
