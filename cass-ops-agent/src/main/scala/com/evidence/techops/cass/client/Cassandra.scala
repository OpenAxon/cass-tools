package com.evidence.techops.cass.client

import java.security.cert
import javax.net.ssl.{X509TrustManager, TrustManager, SSLContext}

import com.datastax.driver.core.{SSLOptions, ProtocolOptions, Cluster}
import com.evidence.techops.cass.agent.config.ServiceConfig
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConversions._

/**
 * Created by pmahendra on 11/20/15.
 */


object Cassandra extends LazyLogging {
  def connect(cassandraConfig: ServiceConfig, doTrustAllCerts: Boolean = false): Cluster = {

    logger.info(s"${cassandraConfig.getCassRpcHost()} ${cassandraConfig.getCassPort()} ${cassandraConfig.getCassUsername()} ${cassandraConfig.getCassPassword()}")

    var builder =
      Cluster.builder()
        .withPort(cassandraConfig.getCassPort())
        .addContactPoint(cassandraConfig.getCassRpcHost())
        .withCompression(ProtocolOptions.Compression.LZ4)
        .withCredentials(cassandraConfig.getCassUsername(), cassandraConfig.getCassPassword())

    if(cassandraConfig.getCassOverTls()) {
      if( doTrustAllCerts ) {
        val trustAllCerts = Array(new TrustManager {
          new X509TrustManager {
            override def getAcceptedIssuers: Array[cert.X509Certificate] = {
              return new Array[cert.X509Certificate](0)
            }

            override def checkClientTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}

            override def checkServerTrusted(x509Certificates: Array[cert.X509Certificate], s: String): Unit = {}
          }
        })

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustAllCerts, null)
        val sslOptions = new SSLOptions(sslContext, SSLOptions.DEFAULT_SSL_CIPHER_SUITES)

        builder = builder.withSSL(sslOptions)
      } else {
        builder = builder.withSSL()
      }
    }

    val cluster = builder.build()

    val metadata = cluster.getMetadata
    logger.info("Connected to Cassandra Cluster {}. {} Hosts", metadata.getClusterName, metadata.getAllHosts.size().toString)

    metadata.getAllHosts.foreach(host =>
      logger.info("Datacenter: {}; Host: {}; Rack: {}", host.getDatacenter, host.getAddress, host.getRack)
    )

    cluster
  }
}