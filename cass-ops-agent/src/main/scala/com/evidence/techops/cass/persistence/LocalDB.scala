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

package com.evidence.techops.cass.persistence

import com.evidence.techops.cass.agent.config.ServiceConfig

import scala.slick.driver.SQLiteDriver.simple._
import java.util.Date
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.slick.jdbc.meta.MTable
import com.typesafe.scalalogging.LazyLogging

/**
 * Created by pmahendra on 9/8/14.
 */

class LocalDB(config: ServiceConfig, dbName:String) extends LazyLogging {
  private var database:Database = null
  private implicit var session:Session = null

  case class ServiceStateTableRow(name: String, value:String, dateModified:Long)

  def init(): LocalDB = {
    logger.debug("Initialize: service_state")
    database = Database.forURL(s"jdbc:sqlite:${config.getAgentStateDataFolder()}/%s.db" format dbName, driver = "org.sqlite.JDBC")
    session = database.createSession()

    if( MTable.getTables("service_state").list.isEmpty) {
      Q.updateNA("""CREATE TABLE service_state (name varchar primary key not null, value int not null, date_modified long not null)""").execute
      logger.debug("Initialize: service_state created")
    } else {
      logger.debug("Initialize: service_state exists")
    }

    this
  }

  def saveState(name:String, value:String): Unit = {
    val replaceSql = Q.update[(String,String,Long)]("REPLACE INTO service_state (name, value, date_modified) VALUES (?, ?, ?)")
    replaceSql((name, value, (new Date()).getTime())).first
  }

  def getStateOpt(name:String): Option[String] = {
    implicit val getSupplierResult = GetResult(r => ServiceStateTableRow(r.nextString, r.nextString, r.nextLong))
    val selectQ = Q[String, ServiceStateTableRow] + "select * from service_state where name = ?"

    selectQ(name).firstOption match {
      case Some(row) =>
        logger.info(s"${row.name} = ${row.value} (date_modified: ${row.dateModified}})")
        Some(row.value)
      case None =>
        None
    }
  }
}

object LocalDB
{
  def apply(config: ServiceConfig, dbName: String): LocalDB = {
    new LocalDB(config, dbName).init()
  }
}
