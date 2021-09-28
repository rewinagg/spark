CREATE EXTERNAL TABLE tweets(
 id String,
 hashTages String,
 count String
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
 "hbase.columns.mapping" = ":key, cf_hash:hashTages, cf_count:count"
)
TBLPROPERTIES(
 "hbase.table.name" = "tweets",
 "hbase.mapred.output.outputtable" = "tweets"
);
