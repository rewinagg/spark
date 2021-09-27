package consumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseConfig implements Serializable{
	private static final String TABLE_NAME = "tweets";
  	private static final String CF_HASH = "cf_hash";
  	private static final String CF_COUNT = "cf_count";
    private static HbaseConfig hbaseInstance;
    private static final String HashTag = "tweet";
	private static final String Count = "Count";
    private static Table tweetsTable;
    
  	public static HbaseConfig getInstance() throws IOException {
        if (hbaseInstance != null)
            return hbaseInstance;
        
        return new HbaseConfig();
    }
    
    private HbaseConfig() throws IOException {
        createTable();
    }
    
   Configuration config = HBaseConfiguration.create();
      
  		public void createTable(String... args) throws IOException
  		{
  			try
  	        {
//  				config.set("hbase.zookeeper.quorum", "localhost");
//  			    config.set("hbase.zookeeper.property.clientPort", "5181");
  	            Connection connection = ConnectionFactory.createConnection(config);
  	            Admin admin = connection.getAdmin();
  	            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
  	            table.addFamily(new HColumnDescriptor(Count).setCompressionType(Algorithm.NONE));
  			    table.addFamily(new HColumnDescriptor(HashTag).setCompressionType(Algorithm.NONE));

  			   System.out.print("Creating table.... ");

  			if (admin.tableExists(table.getTableName()))
  			{
  				admin.disableTable(table.getTableName());
  				admin.deleteTable(table.getTableName());
  			}
  			admin.createTable(table);

  			tweetsTable = connection.getTable(TableName.valueOf(TABLE_NAME));
  			
  			System.out.println(" Done!");
  	            
  	        } finally{
  	        	
  	        }
  		}
  		
  		public void addTweet(String str, String count) throws IOException {
  			Put put = new Put(Bytes.toBytes(new Date().getTime() + ""));
  			put.addColumn(Bytes.toBytes(CF_HASH), Bytes.toBytes("hashTages"), Bytes.toBytes(str));
  			put.addColumn(Bytes.toBytes(CF_COUNT), Bytes.toBytes("count"), Bytes.toBytes(count));
  			tweetsTable.put(put);
  		}
  		

}
