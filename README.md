### Cassandra Tools:

- Cass-ops-agent: Cassandra Thrift/Finagle Companion Service to Backup to AWS S3

### Cass-ops-agent Iface:

<pre>
<code>
exception BackupRestoreException
{
  1: optional i64 code,
  2: optional string message
}

service CassOpsAgent
{
    string getStatus(),
    string getColumnFamilyMetric(1: string keySpace, 2: string colFamily),

    string incrementalBackup(1: string keySpace)        throws (1: BackupRestoreException ea),           // sst backup to s3
    string incrementalBackup2(1: string keySpace)       throws (1: BackupRestoreException ea),           // sst backup compressed to s3
    string snapshotBackup(1: string keySpace)           throws (1: BackupRestoreException ea),           // snapshot directory to s3
    string snapshotBackup2(1: string keySpace)          throws (1: BackupRestoreException ea),           // snapshot directory compressed -> s3
    string commitLogBackup()                            throws (1: BackupRestoreException ea),           // commitlogs directory to s3
    string commitLogBackup2()                           throws (1: BackupRestoreException ea),           // commitlogs directory compressed -> s3

    void   restoreBackup(1: string keySpace, 2: string snapShotName, 3: string hostId)  throws (1: BackupRestoreException ea)  // host id can be null. defaults to current node host id if null
}
</code>
</pre>

### Cass-ops-agent command line tool:

### cass-ops-cli.py -h

<pre>
<code>

usage: cass-ops-cli.py [-h] [-keyspace KEYSPACE] [-cf CF]
                       [-partitioner PARTITIONER]
                       [-cmd {status,snap,snap2,sst,sst2,cl,cl2,restore,nrpe}]
                       [-host HOST] [-port PORT] [-tls] [-snap SNAP]
                       [-csv CSV] [-sstable SSTABLE] [-hostid HOSTID] [-v]
                       [-check {live_nodes,snap_backup_age,sst_backup_age,cl_backup_age,heap_usage_perc,write_latency,read_latency}]
                       [-crit CRIT] [-warn WARN]

EVIDENCE.com Cassandra Ops Cli

optional arguments:
  -h, --help            show this help message and exit
  -keyspace KEYSPACE    Cassandra keyspace name
  -cf CF                Cassandra column family name
  -partitioner PARTITIONER
                        Cassandra partitioner to use (eg: Murmur3Partitioner)
  -cmd {status,snap,snap2,sst,sst2,cl,cl2,restore,nrpe}
                        Backup/restore commands
  -host HOST            Cassandra hostname. Defaults to localhost
  -port PORT            Cassandra hostname port. Defaults to 9123
  -tls                  Use TLS for transport.
  -snap SNAP            Cassandra snapshot name to restore
  -csv CSV              Comma separated data file to convert to sstable
  -sstable SSTABLE      SSTable folder load in to cassandra
  -hostid HOSTID        Host ID value (for restoring backups)
  -v, --verbose         Verbose output

nrpe:
  -check {live_nodes,snap_backup_age,sst_backup_age,cl_backup_age,heap_usage_perc,write_latency,read_latency}
                        NRPE checks to execute
  -crit CRIT            NRPE check critical threshold
  -warn WARN            NRPE check warning threshold

</code>
</pre>

