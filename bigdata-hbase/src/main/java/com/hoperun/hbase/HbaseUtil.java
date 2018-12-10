package com.hoperun.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil
{
    public static Configuration config;
    public static Admin admin;
    static
    {
	config = HBaseConfiguration.create();

	// 数据库元数据操作对象
	// Admin admin;

	// 取得一个数据库连接的配置参数对象

	// 设置连接参数：HBase数据库所在的主机IP
	config.set("zookeeper.znode.parent", "/hbase");
	config.set("hbase.zookeeper.quorum", "10.20.28.146");
	// 设置连接参数：HBase数据库使用的端口
	config.set("hbase.zookeeper.property.clientPort", "2181");
	// config.set("hbase.master", "10.20.28.146:60010");
	try
	{
	    admin = ConnectionFactory.createConnection(config).getAdmin();
	} catch (IOException e)
	{
	    e.printStackTrace();
	}
    }

    /**
     * 创建表
     * 
     * @param tableName
     *            表名
     * @param columns
     *            列族
     * @return
     */
    @SuppressWarnings("deprecation")
    public boolean createTable(String tableName, String[] columns)
    {
	boolean result = false;
	try
	{
	    if (admin.listTableNames(tableName) != null)
	    {
		System.out.println("表已经存在！");
		result = false;
	    } else
	    {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String column : columns)
		{
		    desc.addFamily(new HColumnDescriptor(column));
		}
		admin.createTable(desc);
		System.out.println("表创建成功！");
		result = true;
	    }
	} catch (Exception e)
	{
	    result = false;
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 清空表
     * 
     * @param tabName
     * @return true 表示成功
     */
    public boolean truncateTable(String tabName)
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------清空表 START-----------------");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf(tabName);
	    // 设置表状态为无效
	    admin.disableTable(tableName);
	    // 清空指定表的数据
	    admin.truncateTable(tableName, true);
	    System.out.println("---------------清空表 End-----------------");
	    result = true;
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 删除表
     * 
     * @param tabName
     * @return true 删除成功;
     */
    public boolean deleteTable(String tabName)
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------删除表 START-----------------");
	    // 设置表状态为无效
	    admin.disableTable(TableName.valueOf(tabName));
	    // 删除指定的数据表
	    admin.deleteTable(TableName.valueOf(tabName));
	    System.out.println("---------------删除表 End-----------------");
	    result = true;
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 删除行
     * 
     * @param tabName
     * @param rowkey
     * @return
     * @throws IOException
     */
    public boolean deleteByRowKey(String tabName, String rowkey) throws IOException
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------删除行 START-----------------");
	    // 取得待操作的数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 创建删除条件对象
	    Delete delete = new Delete(Bytes.toBytes(rowkey));
	    // 执行删除操作
	    table.delete(delete);
	    System.out.println("---------------删除行 End-----------------");
	    result = true;
	} catch (Exception e)
	{
	    result = false;
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 新建列族
     * 
     * @param tabName
     * @param fName
     * @return
     * @throws IOException
     */
    public boolean addColumnFamily(String tabName, String fName)
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------新建列族 START-----------------");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf("t_book");
	    // 创建列族对象
	    HColumnDescriptor columnDescriptor = new HColumnDescriptor(fName);
	    // 将新创建的列族添加到指定的数据表
	    admin.addColumn(tableName, columnDescriptor);
	    System.out.println("---------------新建列族 END-----------------");
	    result = true;
	} catch (Exception e)
	{
	    result = false;
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 删除列族
     * 
     * @param tabName
     * @param fname
     * @return
     */
    public boolean deleteColumnFamily(String tabName, String fname)
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------删除列族 START-----------------");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf(tabName);
	    // 删除指定数据表中的指定列族
	    admin.deleteColumn(tableName, fname.getBytes());
	    System.out.println("---------------删除列族 END-----------------");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 插入数据
     */
    public boolean insert(String tabName, List<Put> putList)
    {
	boolean result = false;
	try
	{
	    System.out.println("---------------插入数据 START-----------------");
	    Connection connection = ConnectionFactory.createConnection(config);
	    // 取得一个数据表对象
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 将数据集合插入到数据库
	    table.put(putList);
	    System.out.println("---------------插入数据 END-----------------");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 查询整表数据
     */

    public ResultScanner queryAllTable(String tabName)
    {
	ResultScanner resultx = null;
	try
	{
	    System.out.println("---------------查询整表数据 START-----------------");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 取得表中所有数据
	    resultx = table.getScanner(new Scan());
	    // 循环输出表中的数据
	    for (Result result : resultx)
	    {
		byte[] row = result.getRow();
		System.out.println("row key is:" + new String(row));
		List<Cell> listCells = result.listCells();
		for (Cell cell : listCells)
		{
		    byte[] familyArray = cell.getFamilyArray();
		    byte[] qualifierArray = cell.getQualifierArray();
		    byte[] valueArray = cell.getValueArray();
		    System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray)
			    + new String(valueArray));
		}
	    }
	    System.out.println("---------------查询整表数据 END-----------------");

	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return resultx;
    }

    /**
     * 按行键查询表数据
     * 
     * @param tabName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public Result queryTableByRowKey(String tabName, String rowKey)
    {
	Result result = null;
	try
	{
	    if (StringUtils.isEmpty(tabName) || StringUtils.isEmpty(rowKey))
	    {
		return null;
	    }
	    System.out.println("---------------按行键查询表数据 START-----------------");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 新建一个查询对象作为查询条件
	    Get get = new Get(rowKey.getBytes());
	    // 按行键查询数据
	    result = table.get(get);
	    byte[] row = result.getRow();
	    System.out.println("row key is:" + new String(row));
	    List<Cell> listCells = result.listCells();
	    for (Cell cell : listCells)
	    {
		byte[] familyArray = cell.getFamilyArray();
		byte[] qualifierArray = cell.getQualifierArray();
		byte[] valueArray = cell.getValueArray();
		System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray)
			+ new String(valueArray));
	    }
	    System.out.println("---------------按行键查询表数据 END-----------------");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return result;

    }

    /**
     * 按条件查询表数据
     */

    public ResultScanner queryTableByCondition(String tabName, Filter filter)
    {
	ResultScanner resultScanner = null;
	try
	{
	    System.out.println("---------------按条件查询表数据 START-----------------");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 创建一个查询过滤器
	    // Filter filter = new
	    // SingleColumnValueFilter(Bytes.toBytes("base"),
	    // Bytes.toBytes("name"), CompareOp.EQUAL,
	    // Bytes.toBytes("bookName6"));
	    // 创建一个数据表扫描器
	    Scan scan = new Scan();
	    // 将查询过滤器加入到数据表扫描器对象
	    scan.setFilter(filter);
	    // 执行查询操作，并取得查询结果
	    resultScanner = table.getScanner(scan);

	    // 循环输出查询结果
	    for (Result result : resultScanner)
	    {
		byte[] row = result.getRow();
		System.out.println("row key is:" + new String(row));
		List<Cell> listCells = result.listCells();
		for (Cell cell : listCells)
		{
		    System.out.println("family:" + Bytes.toString(CellUtil.cloneFamily(cell)));
		    System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
		    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
	    }

	    System.out.println("---------------按条件查询表数据 END-----------------");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return resultScanner;
    }

    public static void main(String[] args) throws IOException
    {
	// 第一步，设置HBsae配置信息
	Configuration configuration = HBaseConfiguration.create();
	// 注意。这里这行目前没有注释掉的，这行和问题3有关系 是要根据自己zookeeper.znode.parent的配置信息进行修改。
	configuration.set("zookeeper.znode.parent", "/hbase"); 
	configuration.set("hbase.zookeeper.quorum", "10.20.28.146");
	configuration.set("hbase.zookeeper.property.clientPort", "2181");
	Admin admin = ConnectionFactory.createConnection(configuration).getAdmin();
	if (admin != null)
	{
	    try
	    {
		// 获取到数据库所有表信息
		HTableDescriptor[] allTable = admin.listTables();
		for (HTableDescriptor hTableDescriptor : allTable)
		{
		    System.out.println(hTableDescriptor.getNameAsString());
		}
	    } catch (IOException e)
	    {
		e.printStackTrace();
	    }
	}
    }

}
