#!/bin/sh

#echo configuring hbase ...
#hbase shell < hbase-conf/db

 service hbase-master restart
 service hbase-regionserver restart

clean=false
while getopts 'c' opt; do
	case $opt in
		c) clean=true;;
		*) echo "Error unknown arg!" exit 1
	esac
done

echo configuring hbase ...
if "$clean"; 
	then if [[ $(echo "exists 'tweets'" | hbase shell | grep 'does not exist') ]];
		then echo "create 'tweets', 'cf_hash', 'cf_count'" | hbase shell;
	else
		echo "deleting existing table"
		echo "disable 'tweets'" | hbase shell
		echo "drop 'tweets'" | hbase shell
		echo "create 'tweets',  'cf_hash', 'cf_count'" | hbase shell
	fi
else
	if [[ $(echo "exists 'tweets'" | hbase shell | grep 'does not exist') ]];
		then echo "create 'tweets',  'cf_hash', 'cf_count'" | hbase shell;
	else 
		echo "Table already exists"
	fi
fi

echo configuring hive ...
if "$clean";
	then if [[ $(echo "show tables like 'tweets'" | hive | grep 'tweets') ]];
		then echo "deleting old table"
		hive -e "drop table tweets"
		hive -f ../hive-conf/db.hql;
	else
		hive -f ../hive-conf/db.hql
	fi
else
	hive -f ../hive-conf/db.hql
fi 






