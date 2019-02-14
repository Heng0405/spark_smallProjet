package com.spark.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * singleton
 */
public class HbaseUtils {

	HBaseAdmin  admin = null;
	Configuration configuration = null;

	private HbaseUtils(){
		configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum","hadoop000:2181");
		configuration.set("hbase.rootdir","hdfs://hadoop000:8020/hbase");

		try {
			admin = new HBaseAdmin(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private static HbaseUtils instance=null;
	public  static synchronized  HbaseUtils getInstance(){
		if(null==instance){
			instance=new HbaseUtils();
		}

		return instance;
	}

	public HTable getTable(String tableName){
		HTable table =null;
		try {
			table = new HTable(configuration,tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return table;
	}

	public void put(String tableName,String rowKey,String cf,String  column, String value){
		HTable table=getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		//HTable table = HbaseUtils.getInstance().getTable("course_click_count");

		//System.out.println(table.getName().getNameAsString());

		String tableName="course_click_count";
		String rowKey="20171111_88";
		String cf="info";
		String column= "click_count";
		String value = "2";

		HbaseUtils.getInstance().put(tableName,rowKey,cf,column,value);




	}
}
