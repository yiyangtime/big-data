package com.hoperun.hbase.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hoperun.hbase.common.HBaseUtil;
import com.hoperun.hbase.facade.HBaseUtilService;

@Service("hBaseUtilService")
public class HBaseUtilServiceImpl implements HBaseUtilService
{
    @Autowired
    private HBaseUtil hBaseUtil;

    public void createTable() throws IOException
    {
	String[] columns = new String[] { "id", "name" };
	hBaseUtil.createTable("info_user", columns);
    }

    public void insertTest()
    {
	List<Put> putList = new ArrayList<Put>();
	Put put;
	for (int i = 0; i < 2; i++)
	{
	    put = new Put(Bytes.toBytes("row" + i));
	    put.addColumn(Bytes.toBytes("id"), null, Bytes.toBytes("id" + i));
	    put.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("name" + i));
	    putList.add(put);
	}
	hBaseUtil.insert("info_user", putList);

    }

    public void tableExists()
    {
	hBaseUtil.init();
	String tableName = "info_user";
	try
	{
	    @SuppressWarnings("static-access")
	    Boolean bool = hBaseUtil.tableExists(tableName);
	    System.out.println(bool);
	} catch (IOException e)
	{
	    e.printStackTrace();
	}
    }
}
