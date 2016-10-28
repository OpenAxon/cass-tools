#!/usr/bin/sh

#
# sql-to-cassandra-migration.sh
# Shell script for orchestrating csv.gz -> csv -> sstables -> import into cassandra DB
# Created by pmahendra on 10/16/2014.
#

keyspace=$1
cf_name=$2
tmp_folder="/media/ephemeral0/cass-ops-agent"
cass_cli=/usr/local/bin/cass-ops-cli-1.0.0/cass-ops-cli.py

if ["$keyspace" == "" || "$cf_name" == ""]; then
        echo "Usage <script> keyspace column_family"
        echo
        exit
fi

echo
echo "Cluster info for: $keyspace.$cf_name"
echo
echo `$cass_cli -cmd status`
echo

read -p "Ready to download and convert $cf_name.csv? [y/n] " -n 1 -r
echo    # (optional) move to a new line

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo `$cass_cli -cmd csv2sstable -keyspace $keyspace -cf $cf_name -partitioner Murmur3Partitioner -csv s3://$cf_name.csv.gz`
else
    exit
fi

echo
echo "Conversion of $cases_events_feed.csv done!"
csv_files=$tmp_folder/csv-files/$keyspace/$cf_name/*
echo "Cleanup csv files $csv_files"
echo `rm -Rfv $csv_files`
echo

read -p "Ready to import into cass table $cf_name? [y/n] " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo `python $cass_cli -cmd sstableload -keyspace $keyspace -cf $cf_name -sstable $tmp_folder/sstable-output/$keyspace/$cf_name`
else
    exit
fi

echo
echo "Import done! Sstables source folder is now:"
echo `ls -lh $tmp_folder/sstable-output/$keyspace/$cf_name/`
echo

read -p "Cleanup sstable output files for $cf_name? [y/n] " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
        echo
        sstable_processed_files=$tmp_folder/sstable-processed/$keyspace/$cf_name/*
        echo "Cleaning up $sstable_processed_files ...."
        echo `rm -Rfv $sstable_processed_files`
        echo
else
    exit
fi