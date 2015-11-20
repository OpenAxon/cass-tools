package com.evidence.techops.cass.statsd

import com.evidence.techops.cass.agent.config.ServiceConfig
import com.timgroup.statsd.{StatsDClient, NonBlockingStatsDClient}
import com.typesafe.scalalogging.LazyLogging

/**
 * Created by pmahendra on 11/19/15.
 */

object StatsdClient extends LazyLogging with StatsD {

  private var statsdClient: NonBlockingStatsDClient = null

  def statsd: StatsDClient = {
    statsdClient
  }

  def startupStatsd(serviceConfig: ServiceConfig): Unit = {
    if (serviceConfig.statsdEnabled) {
      startupStatsd("edc.cass_ops_agent", serviceConfig.statsdHost, serviceConfig.statsdPort)
    }
  }

  def startupStatsd(processName: String, host: String = "localhost", port: Int = 8125): Unit = {
    if (statsd != null) {
      statsd.stop()
    }

    statsdClient = new NonBlockingStatsDClient(processName, host, port)
  }

  def shutdownStatsd() = {
    if (statsd != null) {
      statsd.stop()
    }
  }
}
