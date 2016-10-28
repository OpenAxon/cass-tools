#!/usr/bin/python

import sys

sys.path.append('gen-py')
sys.path.append('/opt/cass-ops-agent/cass-ops-agent/bin/cmdline-tool/gen-py')
sys.path.append('/opt/cass-ops-agent/cass-ops-agent/bin/cmdline-tool/thrift')

import com.evidence.techops.cass.ttypes
import com.evidence.techops.cass.CassOpsAgent
import argparse
import json
import logging

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TSSLSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Axon/EVIDENCE.com Cassandra Ops Cli')

parser.add_argument('-keyspace', default="", help='Cassandra keyspace name')
parser.add_argument('-cf', default="", help='Cassandra column family name')
parser.add_argument('-partitioner', default="", help='Cassandra partitioner to use (eg: Murmur3Partitioner)')
parser.add_argument('-cmd', default="status", help='Backup/restore commands', choices=['status', 'cfmetric', 'snap', 'snap2', 'sst', 'sst2', 'cl', 'cl2', 'restore', 'csv2sstable', 'sstableload', 'csv2sstable+sstableload', 'nrpe'])
parser.add_argument('-host', default="localhost", help='Cassandra hostname. Defaults to localhost')
parser.add_argument('-port', default=9123, help='Cassandra hostname port. Defaults to 9123')
parser.add_argument('-tls', default=True, help='Use TLS for transport.', choices=['True', 'False'])
parser.add_argument('-snap', default="", help='Cassandra snapshot name to restore')
parser.add_argument('-csv', default="", help='Comma separated data file to convert to sstable')
parser.add_argument('-sstable', default="", help='SSTable folder load in to cassandra')
parser.add_argument('-hostid', default="", help='Host ID value (for restoring backups)')
parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")

nrpe_group = parser.add_argument_group('nrpe')
nrpe_group.add_argument('-check', default="", help='NRPE checks to execute',
                        choices=['live_nodes', 'snap_backup_age', 'sst_backup_age', 'cl_backup_age', 'heap_usage_perc',
                                 'write_latency', 'read_latency'])
nrpe_group.add_argument('-crit', default=0, help='NRPE check critical threshold')
nrpe_group.add_argument('-warn', default=0, help='NRPE check warning threshold')

args = parser.parse_args()

if args.verbose:
    logger.setLevel(logging.DEBUG)

# Plugin Return Code	Service State	Host State
# 0	OK	UP
# 1	WARNING	UP or DOWN/UNREACHABLE*
# 2	CRITICAL	DOWN/UNREACHABLE
# 3	UNKNOWN	DOWN/UNREACHABLE


def dump_json_data(json_data):
    logger.debug("response = %s", json.dumps(json_data, sort_keys=True, indent=4, separators=(',', ': ')))


def nrpe_live_nodes(client, warning, critical):
    rv = json.loads(client.getStatus())
    dump_json_data(rv)
    live_nodes_count = len(rv["live_nodes"])
    logger.debug("live nodes count: %s", live_nodes_count)

    if live_nodes_count <= critical:
        print "CRITICAL: live nodes count: %d/%d" % (live_nodes_count, critical)
        sys.exit(2)
    elif live_nodes_count <= warn:
        print "WARNING: live nodes count: %d/%d" % (live_nodes_count, warning)
        sys.exit(1)
    else:
        print "OK: live nodes count: %d/%d" % (live_nodes_count, warning)
        sys.exit(0)


def nrpe_sst_backup_age(client, warning, critical):
    rv = json.loads(client.getStatus())
    dump_json_data(rv)
    last_sst_backup_age_mins = int(rv["last_sst_backup_age_mins"])

    if last_sst_backup_age_mins < 0 or last_sst_backup_age_mins >= critical:
        print "CRITICAL: last_sst_backup_age_mins: %d/%d" % (last_sst_backup_age_mins, critical)
        sys.exit(2)
    elif last_sst_backup_age_mins >= warning:
        print "WARNING: last_snap_backup_age_mins: %d/%d" % (last_sst_backup_age_mins, warning)
        sys.exit(1)
    else:
        print "OK: last_sst_backup_age_mins: %d/%d" % (last_sst_backup_age_mins, warning)
        sys.exit(0)


def nrpe_snap_backup_age(client, warning, critical):
    rv = json.loads(client.getStatus())
    dump_json_data(rv)
    last_snap_backup_age_mins = int(rv["last_snap_backup_age_mins"])

    if last_snap_backup_age_mins < 0 or last_snap_backup_age_mins >= critical:
        print "CRITICAL: last_snap_backup_age_mins: %d/%d" % (last_snap_backup_age_mins, critical)
        sys.exit(2)
    elif last_snap_backup_age_mins >= warning:
        print "WARNING: last_snap_backup_age_mins: %d/%d" % (last_snap_backup_age_mins, warning)
        sys.exit(1)
    else:
        print "OK: last_snap_backup_age_mins: %d/%d" % (last_snap_backup_age_mins, warning)
        sys.exit(0)


def nrpe_cl_backup_age(client, warning, critical):
    rv = json.loads(client.getStatus())
    dump_json_data(rv)
    last_cl_backup_age_mins = int(rv["last_cl_backup_age_mins"])

    if last_cl_backup_age_mins < 0 or last_cl_backup_age_mins >= critical:
        print "CRITICAL: last_cl_backup_age_mins: %d/%d" % (last_cl_backup_age_mins, critical)
        sys.exit(2)
    elif last_cl_backup_age_mins >= warning:
        print "WARNING: last_cl_backup_age_mins: %d/%d" % (last_cl_backup_age_mins, warning)
        sys.exit(1)
    else:
        print "OK: last_cl_backup_age_mins: %d/%d" % (last_cl_backup_age_mins, warning)
        sys.exit(0)


def nrpe_heap_usage(client, warning, critical):
    rv = json.loads(client.getStatus())
    dump_json_data(rv)
    heap_usage_perc = int(rv["heap_usage_perc"])

    if heap_usage_perc >= critical:
        print "CRITICAL: heap_usage_perc: %d/%d" % (heap_usage_perc, critical)
        sys.exit(2)
    elif heap_usage_perc >= warning:
        print "WARNING: heap_usage_perc: %d/%d" % (heap_usage_perc, warning)
        sys.exit(1)
    else:
        print "OK: heap_usage_perc: %d/%d" % (heap_usage_perc, warning)
        sys.exit(0)


def nrpe_write_latency(client, warning, critical, column_family, keyspace):

    if column_family == "" or keyspace == "":
        print "UNKNOWN: write_latency for ks: %s cf: %s" % (keyspace, column_family)
        sys.exit(3)

    rv = json.loads(client.getColumnFamilyMetric(keyspace, column_family))
    dump_json_data(rv)
    latency = float(rv["WriteLatency"]["mean"])

    if latency >= critical:
        print "CRITICAL: write_latency: %d/%d" % (latency, critical)
        sys.exit(2)
    elif latency >= warning:
        print "WARNING: write_latency: %d/%d" % (latency, warning)
        sys.exit(1)
    else:
        print "OK: write_latency: %d/%d" % (latency, warning)
        sys.exit(0)


def nrpe_read_latency(client, warning, critical, column_family, keyspace):

    if column_family == "" or keyspace == "":
        print "UNKNOWN: read_latency for ks: %s cf: %s" % (keyspace, column_family)
        sys.exit(3)

    rv = json.loads(client.getColumnFamilyMetric(keyspace, column_family))
    dump_json_data(rv)
    latency = float(rv["ReadLatency"]["mean"])

    if latency >= critical:
        print "CRITICAL: read_latency: %d/%d" % (latency, critical)
        sys.exit(2)
    elif latency >= warning:
        print "WARNING: read_latency: %d/%d" % (latency, warning)
        sys.exit(1)
    else:
        print "OK: read_latency: %d/%d" % (latency, warning)
        sys.exit(0)


try:
    logger.info('cass-ops-cli (cmd: %s): connecting to %s:%s tls: %s', args.cmd, args.host, args.port, args.tls)

    if args.tls == True:
        transport = TSSLSocket.TSSLSocket(host=args.host, port=args.port, validate=False)
    else:
        transport = TSocket.TSocket(host=args.host, port=args.port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TFramedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    agent_client = com.evidence.techops.cass.CassOpsAgent.Client(protocol)

    # Connect!
    transport.open()

    try:
        if args.cmd == "status":
            logger.debug('Starting status()')
            status = agent_client.getStatus()
            status = json.loads(status)
            logger.info("response = %s", json.dumps(status, sort_keys=True, indent=4, separators=(',', ': ')))
        elif args.cmd == "cfmetric":
            if args.keyspace != "" and args.cf != "":
                logger.info('Starting getColumnFamilyMetric() keyspace = "%s" cf = "%s"', args.keyspace, args.cf)
                cfmetric = agent_client.getColumnFamilyMetric(args.keyspace, args.cf)
                cfmetric = json.loads(cfmetric)
                dump_json_data(cfmetric)
            else:
                print 'getColumnFamilyMetric() [ERROR!] specify a -keyspace and -cf'
        elif args.cmd == "snap":
            logger.info('Starting snapshotBackup() of keyspace %s', args.keyspace)
            if args.keyspace != "":
                snapShotName = agent_client.snapshotBackup(args.keyspace)
                logger.info('snapshotBackup() name = "%s" [OK]', snapShotName)
            else:
                print 'snapshotBackup() name = "%s" [ERROR] -keyspace required'
        elif args.cmd == "snap2":
            logger.info('Starting snapshotBackup2() of keyspace %s', args.keyspace)
            if args.keyspace != "":
                snapShotName = agent_client.snapshotBackup2(args.keyspace)
                logger.info('snapshotBackup2() name = "%s" [OK]', snapShotName)
            else:
                print 'snapshotBackup2() name = "%s" [ERROR] -keyspace required'
        elif args.cmd == "sst":
            logger.info('Starting incrementalBackup() of keyspace %s', args.keyspace)
            if args.keyspace != "":
                incrBackupName = agent_client.incrementalBackup(args.keyspace)
                logger.info('incrementalBackup() name = "%s" [OK]', incrBackupName)
            else:
                print 'incrementalBackup() name = "%s" [ERROR] -keyspace required'
        elif args.cmd == "sst2":
            logger.info('Starting incrementalBackup2() of keyspace %s', args.keyspace)
            if args.keyspace != "":
                incrBackupName = agent_client.incrementalBackup2(args.keyspace)
                logger.info('incrementalBackup2() name = "%s" [OK]', incrBackupName)
            else:
                print 'incrementalBackup2() name = "%s" [ERROR] -keyspace required'
        elif args.cmd == "cl":
            logger.info('Starting commitLogBackup()')
            snapShotName = agent_client.commitLogBackup()
            logger.info('commitLogBackup() name = "%s" [OK]', snapShotName)
        elif args.cmd == "cl2":
            logger.info('Starting commitLogBackup2()')
            snapShotName = agent_client.commitLogBackup2()
            logger.info('commitLogBackup2() name = "%s" [OK]', snapShotName)
        elif args.cmd == "restore":
            if args.snap != "" and args.keyspace != "":
                logger.info('Starting restoreBackup() name = "%s"', args.snap)
                snapShotName = agent_client.restoreBackup(args.keyspace, args.snap, args.hostid)
                logger.info('restoreBackup() name = "%s" [OK]', args.snap)
            else:
                print 'restoreBackup() [ERROR!] specify a -keyspace and -snap (snapshot name) to restore'
        elif args.cmd == "nrpe":
            if args.check != "":

                warn = float(args.warn)
                crit = float(args.crit)

                if args.check == "live_nodes":
                    nrpe_live_nodes(agent_client, warn, crit)
                elif args.check == "snap_backup_age":
                    nrpe_snap_backup_age(agent_client, warn, crit)
                elif args.check == "sst_backup_age":
                    nrpe_sst_backup_age(agent_client, warn, crit)
                elif args.check == "cl_backup_age":
                    nrpe_cl_backup_age(agent_client, warn, crit)
                elif args.check == "heap_usage_perc":
                    nrpe_heap_usage(agent_client, warn, crit)
                elif args.check == "write_latency":
                    nrpe_write_latency(agent_client, warn, crit, args.cf, args.keyspace)
                elif args.check == "read_latency":
                    nrpe_read_latency(agent_client, warn, crit, args.cf, args.keyspace)
                else:
                    print "UNKNOWN nrpe check: %s" % args.check
                    sys.exit(3)
            else:
                print 'nrpe [error] specify a -check value'
                sys.exit(3)

    except Thrift.TException, tx:
        logger.error('exception: %s', tx)
        sys.exit(3)


except Thrift.TException, tx:
    logger.error('%s msg: %s', tx, tx.message)
    sys.exit(3)


