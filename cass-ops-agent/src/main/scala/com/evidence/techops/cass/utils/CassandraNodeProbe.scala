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

package com.evidence.techops.cass.utils

import com.evidence.techops.cass.exceptions.JMXConnectionException
import com.typesafe.scalalogging.LazyLogging
import org.apache.cassandra.tools.NodeProbe

/**
 * Created by pmahendra on 2/14/15.
 */

class CassandraNodeProbe(host: String, port: Int) extends NodeProbe(host, port) with LazyLogging {

}

object CassandraNodeProbe extends LazyLogging
{
  var probe:CassandraNodeProbe = null

  def isConnected():Boolean = {
    if (probe == null)
      return false
    try {
      probe.isInitialized
    }
    catch {
      case ex: Throwable => {
        close
        return false
      }
    }
    return true
  }

  def close() {
    try {
      if( probe != null )
        probe.close
    }
    catch {
      case e: Exception => {
        logger.warn("failed to close jxm node tool", e)
      }
    }
  }

  private def connect(host: String, port: Int):CassandraNodeProbe = {
    var tryCount = 0
    val tryMax = 3

    while(tryCount <= tryMax) {
      try {
        tryCount += 1
        probe = new CassandraNodeProbe(host, port)

        return probe
      }
      catch {
        case e: Exception => {
          logger.error(e.getMessage, e)
          if( tryCount > tryMax ) {
            throw new JMXConnectionException(e.getMessage)
          }
        }
      }
    } //  while(tryCount <= tryMax) ...

    return probe
  }

  def getInstanceOf(host: String, port: Int):CassandraNodeProbe = {
    if (!isConnected()) {
      probe = connect(host, port)
    }

    return probe
  }
}
