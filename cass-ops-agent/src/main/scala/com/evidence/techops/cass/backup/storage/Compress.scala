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

package com.evidence.techops.cass.backup.storage

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.utils.IOUtils
import com.typesafe.scalalogging.LazyLogging

/**
 * Created by pmahendra on 1/8/15.
 */

object Compress extends LazyLogging {
  private val buf = new Array[Byte](1024)

  def createTarGzip(inputDirectoryPath:String, outputFilePath:String):Long = {
    createTarGzip(new File(inputDirectoryPath), new File(outputFilePath))
  }

  def createTarGzip(inputDirectoryOrFile:File, outputFile:File):Long = {
    val func = "createTarGzip()"

    var tarArchiveOutputStream:TarArchiveOutputStream = null
    try {
      logger.info(s"$func creating compressed archive: ${inputDirectoryOrFile.getAbsolutePath} -> ${outputFile.getAbsolutePath}")

      val outputFileStream = new FileOutputStream(outputFile)
      val bufferedOutputFileStream = new BufferedOutputStream(outputFileStream)
      val gzipCompressorOutputStream = new GzipCompressorOutputStream(bufferedOutputFileStream)
      tarArchiveOutputStream = new TarArchiveOutputStream(gzipCompressorOutputStream)
      tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)
      tarArchiveOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR)

      var filesCompressed:Long = 0

      if (inputDirectoryOrFile.isDirectory()) {
        var count = 0
        val children: Seq[File] = inputDirectoryOrFile.listFiles()
        if (children != null) {
          for (child: File <- children) {
            count += 1
            filesCompressed = filesCompressed + addFileToTarGz(tarArchiveOutputStream, child, "", count, children.length)
            Thread.`yield`()
          }
        }
      } else {
        filesCompressed = filesCompressed + addFileToTarGz(tarArchiveOutputStream, inputDirectoryOrFile, "", 1, 1);
      }

      filesCompressed
    } catch {
      case e:Throwable => {
        logger.error(e.getMessage, e)
        throw e
      }
    } finally {
      if( tarArchiveOutputStream != null ) {
        tarArchiveOutputStream.finish()
        tarArchiveOutputStream.close()
      }
    }
  }

  private def addFileToTarGz(tarArchiveOutputStream:TarArchiveOutputStream, inputDirectoryOrFile:File, archiveBasePath:String, currentFileNo:Int, filesTotal:Int):Long = {
    val func = "addFileToTarGz()"

    val entryName:String = archiveBasePath ++ inputDirectoryOrFile.getName()
    val tarEntry = new TarArchiveEntry(inputDirectoryOrFile, entryName)
    tarArchiveOutputStream.putArchiveEntry(tarEntry)

    logger.info(s"$func \t ++ entry: ${entryName} ($currentFileNo of $filesTotal / ${inputDirectoryOrFile.length()} bytes})")

    var filesCompressed:Long = 0

    if (inputDirectoryOrFile.isFile()) {
      var inputFileStream:FileInputStream = null
      try {
        inputFileStream = new FileInputStream(inputDirectoryOrFile)
        IOUtils.copy(inputFileStream, tarArchiveOutputStream, 524288)
        tarArchiveOutputStream.closeArchiveEntry()
        filesCompressed = filesCompressed + 1
      } finally {
        inputFileStream.close()
      }
    } else {
      tarArchiveOutputStream.closeArchiveEntry()
      val children:Seq[File] = inputDirectoryOrFile.listFiles()
      var count = 0
      if (children != null){
        for (child:File <- children) {
          count += 1
          filesCompressed = filesCompressed + addFileToTarGz(tarArchiveOutputStream, child, entryName + "/", count, children.length)
          Thread.`yield`()
        }
      }
    }

    filesCompressed
  }
}
