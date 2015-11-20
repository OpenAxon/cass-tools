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

package com.evidence.techops.cass.restore

import com.twitter.util.Future
import org.apache.cassandra.dht.Murmur3Partitioner
import com.evidence.techops.cass.BackupRestoreException
import com.evidence.techops.cass.utils.DataImport
import com.typesafe.scalalogging.LazyLogging

/**
 * Created by pmahendra on 9/22/14.
 */

object SSTableLoader extends LazyLogging {
  def csvToSsTableConv(csvFile:String, keySpace:String, columnFamily:String, partitioner:String):Future[String] = {
    Future {
      Option(partitioner).getOrElse("").toLowerCase() match {
        case "murmur3partitioner" => {
          DataImport.doConvertToSstable(keySpace, columnFamily, csvFile, new Murmur3Partitioner())
        }
        case _ => {
          throw new BackupRestoreException(message = Option(s"Invalid partitioner ${partitioner}"))
        }
      }
    }
  }

  def ssTableImport(ssTable:String, keySpace:String, columnFamily:String):Future[Boolean] = {
    Future {
      DataImport.doImportSstable(keySpace, columnFamily, ssTable)
    }
  }
}


