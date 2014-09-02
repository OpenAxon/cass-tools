/**
 * Copyright 2015 TASER International, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evidence.techops.cass.utils;

/**
 * Created by pmahendra on 9/22/14.
 */

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.amazonaws.util.Base64;
import com.evidence.techops.cass.BackupRestoreException;
import com.evidence.techops.cass.agent.ServiceGlobal;
import com.evidence.techops.cass.backup.storage.AwsS3;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.tools.MyBulkLoader;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class DataImport
{
    public static Logger logger = LoggerFactory.getLogger(DataImport.class);
    private static boolean cfMetaDataLoaded = false;

    public synchronized static boolean doImportSstable(String keySpace, String columnFamily, String ssTableFilesDirStr) throws IOException, BackupRestoreException
    {
        logger.info("doImportSstable() called for ks: {} cf: {} sstables: {}", keySpace, columnFamily, ssTableFilesDirStr);

        File ssTableFilesDir = new File(ssTableFilesDirStr);
        if( !ssTableFilesDir.exists() || !ssTableFilesDir.isDirectory() || !ssTableFilesDirStr.startsWith(ServiceGlobal.config().getTmpFolder()) ) {
            throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(ssTableFilesDirStr + " not found or invalid"));
        }

        String processedTablesDir = ServiceGlobal.config().getTmpFolder() + "/sstable-processed/" + keySpace + "/" + columnFamily;
        File processedTablesDirectory = new File(processedTablesDir);
        if (!processedTablesDirectory.exists()) {
            processedTablesDirectory.mkdirs();
        }

        int rpcPort = ServiceGlobal.config().getCassRpcPort();
        String rpcHost = ServiceGlobal.config().getCassRpcHost();
        int throttleMbps = ServiceGlobal.config().getSstableLoaderMaxRateMbps();

	    logger.info("doImportSstable() connecting to rpc host: {} port: {}", rpcHost, rpcPort);

        for( File outputGroupDir : ssTableFilesDir.listFiles() ) {
            if(!outputGroupDir.isDirectory())
                continue;

            File outputGroupLeafDir = new File(outputGroupDir.getAbsolutePath() + "/" + keySpace + "/" + columnFamily);

            if(!outputGroupLeafDir.exists() || !outputGroupLeafDir.getAbsolutePath().endsWith("-output-group/" + keySpace + "/" + columnFamily))
                throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(outputGroupLeafDir.getAbsolutePath() + " invalid"));

            logger.info("Bulk load {} throttle: {} host: {} port: {} ssl proto: tls", outputGroupLeafDir.getAbsolutePath(), throttleMbps, rpcHost, rpcPort);
            String[] args = new String[] {"-d", rpcHost,
                    "-p", Integer.toString(rpcPort),
                    "-u", ServiceGlobal.config().getCassUsername(),
                    "-pw", ServiceGlobal.config().getCassPassword(),
                    "-t", Integer.toString(throttleMbps),
                    "--ssl-protocol", "tls",                                        // TODO: Move this to config
                    "-tf", "org.apache.cassandra.thrift.SSLTransportFactory",       // TODO: Move this to config
                    "-ks", "/opt/cassandra/security/.keystore",                     // TODO: Move this to config
                    "-kspw", "cassandra",                                           // TODO: Move this to config
                    "-ts", "/opt/cassandra/security/.keystore",                     // TODO: Move this to config
                    "-tspw", "cassandra",                                           // TODO: Move this to config
                    outputGroupLeafDir.getAbsolutePath()};

            MyBulkLoader.doBulkLoad(args);

            // move to processed directory ...
            File destDir = new File(processedTablesDirectory.getAbsolutePath() + "/" + outputGroupDir.getName());

            if( destDir.exists() ) {
                logger.info("Cleanup existing dir: {}", destDir.getAbsolutePath());
                FileUtils.deleteDirectory(destDir);
            }

            logger.info("Move {} -> {}", outputGroupDir.getAbsolutePath(), destDir.getAbsolutePath());
            outputGroupDir.renameTo(destDir);
        }

        logger.info("doImportSstable ({}) done!", ssTableFilesDir.length());
        return true;
    }

    public synchronized static String doConvertToSstable(String keySpace, String columnFamily, String csvFileName, IPartitioner partitioner) throws IOException, BackupRestoreException, InterruptedException, InvalidRequestException
    {
        logger.info("doExportToSstable() called for ks: {} cf: {} csv: {}", keySpace, columnFamily, csvFileName);
        ArrayList<String> outputDirs = new ArrayList<String>();

        File csvFileLocal = null;
        // check csv file ... download from s3 if necessary ...
        if( csvFileName.toLowerCase().startsWith("s3://") ) {
            String columnFamilyFileName = csvFileName.replaceAll("s3://", "");
            String bucketName = ServiceGlobal.config().getBackupS3BucketName();
            String s3Key = getRemoteCsvFileStoragePath(keySpace, columnFamilyFileName);

            logger.info("looking for {} bucket: {} key: {}", csvFileName, bucketName, s3Key);
            ObjectListing listing = AwsS3.listS3Directory(bucketName, s3Key);

            ProgressIndicator progressIndicator = new ProgressIndicator(columnFamilyFileName);

            for( S3ObjectSummary summary : listing.getObjectSummaries() ) {
                csvFileName = ServiceGlobal.config().getTmpFolder() + "/csv-files/" + keySpace + "/" + columnFamily + "/" + columnFamilyFileName;
                csvFileLocal = new File(csvFileName);

                try {
                    logger.info("found csv files: {} key: {} download to -> {} [downloading ...]", summary.getBucketName(), summary.getKey(), csvFileName);
                    AwsS3.downloadS3Object(bucketName, s3Key, csvFileLocal, progressIndicator);
                    logger.info("found csv files: {} key: {} download to -> {} [downloading done]", summary.getBucketName(), summary.getKey(), csvFileName);
                } catch(Exception e) {
                    logger.warn(e.getMessage(), e);
                    throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply("Error downloading: " + s3Key + " exception: " + e.getMessage()));
                }
            }
        } else {
            csvFileLocal = new File(csvFileName);
        }

        if( !csvFileLocal.exists() || !csvFileLocal.getAbsolutePath().startsWith(ServiceGlobal.config().getTmpFolder()) ) {
            throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(csvFileName + " not found or invalid"));
        }

        // if the file ends with a .gz we need to unzip ...
        byte[] csvFileNameBuffer = new byte[1024000];
        long decompBytes = 0;

        if( csvFileLocal.getName().endsWith(".gz") ) {
            logger.info("decompress: {}", csvFileLocal.getAbsolutePath());

            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(csvFileLocal));

            csvFileLocal = new File(csvFileLocal.getAbsolutePath().replace(".gz", ""));
            logger.info("decompressed file: {}", csvFileLocal.getAbsolutePath());

            FileOutputStream out = new FileOutputStream(csvFileLocal);

            int len;
            DateTime lastLogged = new DateTime();
            while ((len = gzis.read(csvFileNameBuffer)) > 0) {
                decompBytes += len;
                if( (new DateTime()).getMillis() - lastLogged.getMillis() >= 5000 ) {
                    logger.info("decompressed: {} bytes: {} mb", csvFileLocal.getName(), decompBytes / (1024 * 1024));
                    lastLogged = new DateTime();
                }

                out.write(csvFileNameBuffer, 0, len);
            }

            logger.info("decompressed: {} bytes: {} mb", csvFileLocal.getName(), decompBytes / (1024 * 1024));
        }

        logger.info("csv file: {}", csvFileLocal.getAbsolutePath());

        // set system property cassandra.config .... with the yaml file
        System.setProperty("cassandra.config", ServiceGlobal.config().getCassConfigFileUrl());
        System.setProperty("cassandra-rackdc.properties", ServiceGlobal.config().getCassRackDcConfigFileUrl());

        String line;
        long outputGroup = 0;
        int lineNumber = 0;
        int lineNumberLast = 0;
        CsvEntry entry = new CsvEntry();
        Hashtable eventIdHt = new Hashtable();

        cleanupSstableOutputDirectory(keySpace, columnFamily);
        File ssTableOutputDirectory = getSstableOutputDirectory(keySpace, columnFamily, outputGroup);
        outputDirs.add(ssTableOutputDirectory.getAbsolutePath());

        DateTime lastLoggedDateTime = new DateTime();
        DateTime startDateTime = new DateTime();

        Charset charset = Charset.forName("ASCII");
        Path path = FileSystems.getDefault().getPath(csvFileLocal.getAbsolutePath());

        logger.info("open {}", path.getFileName());
        BufferedReader reader = java.nio.file.Files.newBufferedReader(path, charset);

        CQLSSTableWriter ssTableWriter = null;
        String colFamilySchema = null;
        String colFamilyInsertStatement = null;

        line = readLine(reader);

        while (line != null)
        {
            if( lineNumber == 0 ) {
                logger.debug("processing read line: {}", lineNumber);
                entry.parse(csvFileName, line, lineNumber);
                lineNumber++;

                String entityIdType = (String)entry.partitionKeyColumns.get("entity_id");

                if( entityIdType.compareTo("long") == 0 ) {
                    entityIdType = "bigint";
                }

                if( cfMetaDataLoaded == false ) {
                    logger.info("CQLSSTableWriter loadSchemas() ...");
                    DatabaseDescriptor.loadSchemas();
                    cfMetaDataLoaded = true;
                }

                colFamilySchema = String.format("CREATE TABLE %s.%s (" +
                        "     partner_id uuid," +
                        "     entity_id %s, " +
                        "     event_type uuid," +
                        "     event_id timeuuid," +
                        "     event_state int," +
                        "     event text," +
                        "     event_timestamp timestamp," +
                        "     mr_key varchar," +
                        "     primary key ((partner_id, entity_id), event_type, event_state, event_id))", keySpace, columnFamily, entityIdType);

                colFamilyInsertStatement = String.format("INSERT INTO %s.%s (partner_id, entity_id, event_type, event_id, event_state, event, event_timestamp, mr_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", keySpace, columnFamily);

                logger.info("CQLSSTableWriter colFamilySchema: {}", colFamilySchema);
                logger.info("CQLSSTableWriter colFamilyInsertStatement: {}", colFamilyInsertStatement);
                logger.info("CQLSSTableWriter withBufferSizeInMB: {}", ServiceGlobal.config().getSstableWriterBufferSizeMb());

                ssTableWriter = CQLSSTableWriter.builder()
                                                .inDirectory(ssTableOutputDirectory.getAbsolutePath())
                                                .forTable(colFamilySchema)
                                                .withPartitioner(partitioner)
                                                .withBufferSizeInMB(ServiceGlobal.config().getSstableWriterBufferSizeMb())
                                                .using(colFamilyInsertStatement)
                                                .build();


                line = readLine(reader);
                continue;
            } else
            {
                entry.parse(csvFileName, line, lineNumber);

                Object partnerId = entry.columnNameToValueMap.get("partner_id");
                Object entityId = entry.columnNameToValueMap.get("entity_id");
                DateTime eventTimeStamp = (DateTime)entry.columnNameToValueMap.get("event_timestamp");
                Object eventState = entry.columnNameToValueMap.get("event_state");
                Object eventType = entry.columnNameToValueMap.get("event_type");
                Object eventJson = entry.columnNameToValueMap.get("event");
                Object maprKey = entry.columnNameToValueMap.get("mr_key");
                UUID eventId = UUIDGen.getTimeUUID();

                DateTime dateTimeNow = new DateTime();
                int rateCalcInterval = 5000;
                if( dateTimeNow.getMillis() - lastLoggedDateTime.getMillis() > rateCalcInterval ) {
                    lastLoggedDateTime = new DateTime();
                    double elapsed = dateTimeNow.getMillis() - startDateTime.getMillis();
                    double linesRateOverall = 0;
                    double linesRateCurrent = 0;

                    if( elapsed > 0 ) {
                        linesRateOverall = lineNumber / (elapsed / 1000);
                        linesRateCurrent = (lineNumber - lineNumberLast) / (rateCalcInterval / 1000);
                        lineNumberLast = lineNumber;
                    }

                    logger.info("processing read line: {} pid: {} ent: {} evt ts: {} evt state: {} evt type: {} evt id: {} mapr: {} overall rate: {} curr rate {}", lineNumber, partnerId, entityId, eventTimeStamp, eventState, eventType, eventId, maprKey, linesRateOverall, linesRateCurrent);
                }

                DateTime writeLineStart = new DateTime();

                // sanity check ...
                if( ServiceGlobal.config().getDebugMode() == true ) {
                    if (eventIdHt.get(eventId.toString()) != null)
                        throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(eventId + " is a dupe!"));
                    eventIdHt.put(eventId.toString(), true);
                }

                ssTableWriter.addRow(partnerId, entityId, eventType, eventId, eventState, eventJson, eventTimeStamp.toDate(), maprKey);

                long elapsed = (new DateTime()).getMillis() - writeLineStart.getMillis();
                if( elapsed >= 10 ) {
                    logger.warn("cql insert took {} ms on line {}", (new DateTime()).getMillis() - writeLineStart.getMillis(), lineNumber);
                }
                lineNumber++;

                if( ServiceGlobal.config().getSstableWriterMaxRows() > 0 ) {
                    if (lineNumber % ServiceGlobal.config().getSstableWriterMaxRows() == 0) {
                        ssTableWriter.close();

                        outputGroup++;
                        ssTableOutputDirectory = getSstableOutputDirectory(keySpace, columnFamily, outputGroup);
                        outputDirs.add(ssTableOutputDirectory.getAbsolutePath());

                        logger.debug("CQLSSTableWriter ... inDirectory: " + ssTableOutputDirectory.getAbsolutePath() + " @ lineNumber: " + lineNumber);
                        ssTableWriter = CQLSSTableWriter.builder()
                                .inDirectory(ssTableOutputDirectory.getAbsolutePath())
                                .forTable(colFamilySchema)
                                .withPartitioner(partitioner)
                                .withBufferSizeInMB(ServiceGlobal.config().getSstableWriterBufferSizeMb())
                                .using(colFamilyInsertStatement)
                                .build();
                    }
                }
            }

            line = readLine(reader);
            Thread.sleep(0);
        } // while(line != null)

        logger.debug("closing. rows processed total: {} ...", lineNumber - 1);
        ssTableWriter.close();
        logger.info("closing. rows processed total: {} output dirs: {}", lineNumber - 1, outputDirs.size());

        return getSstableOutputDirectoryBase(keySpace, columnFamily);
    }

    private static String readLine(BufferedReader reader) throws IOException
    {
        DateTime readLineStart = new DateTime();
        String line = reader.readLine();
        long elapsed = (new DateTime()).getMillis() - readLineStart.getMillis();
        if( elapsed > 2 ) {
            logger.warn("time to read csv file line {} ms", (new DateTime()).getMillis() - readLineStart.getMillis());
        }

        return line;
    }

    private static String getRemoteCsvFileStoragePath(String keySpace, String columnFamilyFileName)
    {
        String envPath = ServiceGlobal.config().getEnvId() + "-" +
                ServiceGlobal.config().getEnvDeploymentCode() + "-" +
                ServiceGlobal.config().getEnvLocationCode() + "-" +
                ServiceGlobal.config().getEnvServerType() + "-" +
                ServiceGlobal.config().getEnvServerCode();


        return "export/csv/" + envPath + "/" + keySpace + "/" + columnFamilyFileName;
    }

    private static String getSstableOutputDirectoryBase(String keySpace, String columnFamily)
    {
        return ServiceGlobal.config().getTmpFolder() + "/sstable-output/" + keySpace + "/" + columnFamily;
    }

    private static File getSstableOutputDirectory(String keySpace, String columnFamily, long outputGroup) throws BackupRestoreException
    {
        String destinationFolder = getSstableOutputDirectoryBase(keySpace, columnFamily) + "/" + outputGroup + "-sstable-output-group/" + keySpace + "/" + columnFamily;

        File ssTableOutputDirectory = new File(destinationFolder);
        if (!ssTableOutputDirectory.exists())
            ssTableOutputDirectory.mkdirs();

        if (!ssTableOutputDirectory.exists())
            throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(destinationFolder + " not found"));

        return ssTableOutputDirectory;
    }

    private static void cleanupSstableOutputDirectory(String keySpace, String columnFamily) throws BackupRestoreException, IOException
    {
        String destinationFolder = getSstableOutputDirectoryBase(keySpace, columnFamily);

        File ssTableOutputDirectory = new File(destinationFolder);
        if (!ssTableOutputDirectory.exists())
            ssTableOutputDirectory.mkdirs();

        if (!ssTableOutputDirectory.exists())
            throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(destinationFolder + " not found"));

        // cleanup the output directory if it's not empty ..
        for( File file : ssTableOutputDirectory.listFiles() ) {

            if( file.isFile() ) {
                if( !file.delete() ) {
                    throw new IOException("Failed to delete " + file.getAbsolutePath());
                }
            } else {
                FileUtils.deleteDirectory(file);
            }
        }

        if( ssTableOutputDirectory.listFiles().length > 0 )
            throw new BackupRestoreException(Option.apply((Object) new Long(-1)), Option.apply(destinationFolder + " not empty!"));
    }

    static class ProgressIndicator implements S3ProgressListener
    {
        public static Logger logger = LoggerFactory.getLogger(ProgressIndicator.class);

        private long totalBytesTransferred;
        private long contentLength;
        private String fileName;
        private DateTime lastLoggedDt = new DateTime();

        public ProgressIndicator(String fn) {
            totalBytesTransferred = 0;
            fileName = fn;
        }

        public void progressChanged(ProgressEvent progressEvent)
        {
            if( progressEvent.getEventType() == ProgressEventType.RESPONSE_CONTENT_LENGTH_EVENT ) {
                contentLength = progressEvent.getBytes();
                logger.info("file: {} bytes transferred: {} mb of {}", fileName, 0, contentLength / (1024 * 1024));
            } else if( progressEvent.getEventType() == ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT ) {
                totalBytesTransferred += progressEvent.getBytes();
                long megaBytesTransferred = totalBytesTransferred / (1024 * 1024);

                DateTime now = new DateTime();

                if( now.getMillis() - lastLoggedDt.getMillis() > 5000 ) {
                    lastLoggedDt = new DateTime();
                    logger.info("file: {} bytes transferred: {} mb of {}", fileName, megaBytesTransferred, contentLength / (1024 * 1024));
                }
            } else if( progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT ) {
                long megaBytesTransferred = contentLength / (1024 * 1024);
                logger.info("file: {} bytes transferred: {} bytes of {}", fileName, megaBytesTransferred, megaBytesTransferred);
            }
        }

        public void onPersistableTransfer(final PersistableTransfer persistableTransfer)
        {
            // nothing to do ...
        }
    }

    static class CsvEntry
    {
        Hashtable partitionKeyColumns = new Hashtable();
        CompositeType partitionKeyCompositeType = null;
        Hashtable columnNameToTypeMap = new Hashtable();
        Hashtable columnNameToValueMap = new Hashtable();
        String[] columnNames = null;

        void parse(String csvFileName, String line, int lineNumber) throws BackupRestoreException
        {
            if( lineNumber == 0 ) {
                // column names ...
                columnNames = line.split(",");

                if( columnNames == null || columnNames.length == 0 ) {
                    throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply("column names not found"));
                }

                for( int i = 0; i < columnNames.length; i++ ) {
                    String[] columnNameDataTypeArray = columnNames[i].split("=");

                    if( columnNameDataTypeArray == null || columnNameDataTypeArray.length != 2 ) {
                        throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply("column name + type not found"));
                    }

                    String columnName = columnNameDataTypeArray[0];
                    String columnDataType = columnNameDataTypeArray[1];

                    if( columnName.startsWith("*")) {
                        // partition key ...
                        columnName = columnName.replace("*", "");
                        partitionKeyColumns.put(columnName, columnDataType);
                    }

                    columnNameToTypeMap.put(columnName, columnDataType);
                    columnNames[i] = columnName;
                }

                if( partitionKeyColumns.size() == 0 ) {
                    throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply("partitionKeyColumns is 0!"));
                }

                java.util.ArrayList compositeList = new java.util.ArrayList();

                for (Object partitionKey : partitionKeyColumns.keySet()) {
                    String partitionKeyDataType = (String)partitionKeyColumns.get(partitionKey);

                    switch(partitionKeyDataType)
                    {
                        case "uuid":
                            compositeList.add(UUIDType.instance);
                            break;
                        case "long":
                            compositeList.add(LongType.instance);
                            break;
                        case "string":
                            compositeList.add(UTF8Type.instance);
                            break;
                        default:
                            throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply("partitionKeyDataType " + partitionKeyDataType + " type not handled"));
                    }
                }

                partitionKeyCompositeType = CompositeType.getInstance( compositeList );

                return;
            }

            columnNameToValueMap.clear();
            String[] columnValuesFromFile = line.split(",");

            if (columnValuesFromFile.length != columnNameToTypeMap.size())
            {
                logger.error("Invalid input '{}' at line {} in {} col number {} != {}", line, lineNumber, csvFileName, columnValuesFromFile.length, columnNameToTypeMap.size());
                return;
            }
            try
            {
                for( int i = 0; i < columnValuesFromFile.length; i++ ) {
                    String columnName = columnNames[i];
                    String columnDataType = ((String)columnNameToTypeMap.get(columnName)).toLowerCase();

                    Object columnValue;

                    switch(columnDataType)
                    {
                        case "uuid":
                            columnValue = UUID.fromString(columnValuesFromFile[i].trim());
                            break;
                        case "string":
                            columnValue = columnValuesFromFile[i].trim();
                            break;
                        case "base64":
                            columnValue = new String(Base64.decode(columnValuesFromFile[i].trim()));
                            break;
                        case "long":
                            columnValue = Long.parseLong(columnValuesFromFile[i].trim());
                            break;
                        case "int":
                            columnValue = Integer.parseInt(columnValuesFromFile[i].trim());
                            break;
                        case "bool":
                            columnValue = Boolean.parseBoolean(columnValuesFromFile[i].trim());
                            break;
                        case "datetime":
                        case "datetime2":
                            columnValue = DateTime.parse(columnValuesFromFile[i].trim());
                            break;
                        default:
                            throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply("column data type " + columnDataType + " type not handled"));
                    }

                    // sanity check ...
                    if( ServiceGlobal.config().getDebugMode() == true && columnDataType.compareTo("base64") != 0 ) {

                        if(columnDataType.compareTo("datetime") == 0 || columnDataType.compareTo("datetime2") == 0) {
                            if( !columnValuesFromFile[i].trim().startsWith(columnValue.toString().replace("Z", "")) ) {
                                throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply(String.format("%s != %s", columnValue, columnValuesFromFile[i].trim())));
                            }
                        } else if( columnValue.toString().compareTo(columnValuesFromFile[i].trim()) != 0 ) {
                            throw new BackupRestoreException(Option.apply((Object)new Long(-1)), Option.apply(String.format("%s != %s", columnValue, columnValuesFromFile[i].trim())));
                        }
                    }

                    columnNameToValueMap.put(columnName, columnValue);
                }

                return;
            }
            catch (NumberFormatException e)
            {
                logger.error("Invalid number in input '{}' at line %d of {}", line, lineNumber, csvFileName);
                throw e;
            }
        }
    }
}
