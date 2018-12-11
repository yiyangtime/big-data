package com.hoperun.hbase.common;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hoperun.hbase.config.HBaseConfig;

@Component
public class HBaseUtil
{
    private final static Logger LOGGER = LoggerFactory.getLogger(HBaseUtil.class);
    public final static String zookeeperZnodeParent = "zookeeper.znode.parent";
    public final static String hbaseZookeeperQuorum = "hbase.zookeeper.quorum";
    public final static String hbaseZookeeperPropertyClientPort = "hbase.zookeeper.property.clientPort";
    public final static String hbaseMaster = "hbase.master";

    @Autowired
    private static HBaseConfig hBaseConfig;

    private static Configuration config;
    // 设置连接池
    private static ExecutorService pool;
    private static Connection connection;
    private static Admin admin = null;

    public void init()
    {
	config = HBaseConfiguration.create();
	config.set(zookeeperZnodeParent, hBaseConfig.getZookeeperZnodeParent());
	config.set(hbaseZookeeperPropertyClientPort, hBaseConfig.getHbaseZookeeperPropertyClientPort());
	config.set(hbaseZookeeperQuorum, hBaseConfig.getHbaseZookeeperQuorum());
    }

    /**
     * 获得链接
     * 
     * @return
     */
    public static synchronized Connection getConnection()
    {
	try
	{
	    if (connection == null || connection.isClosed())
	    {
		connection = ConnectionFactory.createConnection(config, pool);
	    }
	} catch (IOException e)
	{
	    LOGGER.error("HBase 建立链接失败 ", e);
	}
	return connection;
    }

    /**
     * 根据表名，判断表是否存在
     * 
     * @param tableName
     *            表名
     * @return true 存在 ，false 不存在
     * @throws IOException
     */
    public static boolean tableExists(String tableName) throws IOException
    {
	Connection connection = getConnection();
	Admin admin = connection.getAdmin();
	TableName name = TableName.valueOf(tableName);
	LOGGER.debug("---" + name);
	// 判断表是否存在
	boolean bool = admin.tableExists(name);
	LOGGER.debug(tableName + " exists? " + bool);
	return bool;
    }

    /**
     * 创建带表空间的表
     * 
     * @param nameSpace
     * @param tableName
     * @param family
     * @param splitKeys
     */
    public static void createTable(String nameSpace, String tableName, String family, byte[][] splitKeys)
    {
	try
	{
	    TableName tName = TableName.valueOf(nameSpace, tableName);
	    // 如果表存在，删除表
	    if (admin.tableExists(tName))
	    {
		admin.disableTable(tName);
		admin.deleteTable(tName);
	    } else
	    {

		HTableDescriptor tableDesc = new HTableDescriptor(tName);
		HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
		// 1个版本
		colDesc.setMaxVersions(1);
		// 开启内存缓存
		colDesc.setInMemory(true);
		tableDesc.addFamily(colDesc);
		// 直接创建表
		admin.createTable(tableDesc);
		// 创建表，添加预分区，避免热点写,若不指定splitKeys为空即可
		// admin.createTable(tableDesc, splitKeys);

	    }
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
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    public boolean createTable(String tableName, String[] columns) throws IOException
    {
	Admin admin = connection.getAdmin();
	boolean result = false;
	try
	{
	    if (admin.listTableNames(tableName) != null)
	    {
		LOGGER.error("表已经存在！");
		result = false;
	    } else
	    {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String column : columns)
		{
		    desc.addFamily(new HColumnDescriptor(column));
		}
		admin.createTable(desc);
		LOGGER.debug("表创建成功！");
		result = true;
	    }
	} catch (Exception e)
	{
	    result = false;
	    LOGGER.error("表创建异常!");
	}
	return result;
    }

    /**
     * 清空表
     * 
     * @param tabName
     * @return true 表示成功
     * @throws IOException
     */
    public boolean truncateTable(String tabName) throws IOException
    {
	Admin admin = connection.getAdmin();
	boolean result = false;
	try
	{
	    LOGGER.debug("清空表 START");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf(tabName);
	    // 设置表状态为无效
	    admin.disableTable(tableName);
	    // 清空指定表的数据
	    admin.truncateTable(tableName, true);
	    LOGGER.debug("清空表 End");
	    result = true;
	} catch (Exception e)
	{
	    LOGGER.error("清空表异常！");
	}
	return result;
    }

    /**
     * 删除表
     * 
     * @param tabName
     * @return true 删除成功;
     * @throws IOException
     */
    public boolean deleteTable(String tabName) throws IOException
    {
	Admin admin = connection.getAdmin();
	boolean result = false;
	try
	{
	    LOGGER.debug("删除表 START");
	    // 设置表状态为无效
	    admin.disableTable(TableName.valueOf(tabName));
	    // 删除指定的数据表
	    admin.deleteTable(TableName.valueOf(tabName));
	    LOGGER.debug("删除表 End");
	    result = true;
	} catch (Exception e)
	{
	    LOGGER.error("删除表异常！");
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
	    LOGGER.debug("删除行 START");
	    // 取得待操作的数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 创建删除条件对象
	    Delete delete = new Delete(Bytes.toBytes(rowkey));
	    // 执行删除操作
	    table.delete(delete);
	    LOGGER.debug("删除行 End");
	    result = true;
	} catch (Exception e)
	{
	    result = false;
	    LOGGER.error("删除行异常!");
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
    public boolean addColumnFamily(String tabName, String fName) throws IOException
    {
	Admin admin = connection.getAdmin();
	boolean result = false;
	try
	{
	    LOGGER.debug("新建列族 START");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf("t_book");
	    // 创建列族对象
	    HColumnDescriptor columnDescriptor = new HColumnDescriptor(fName);
	    // 将新创建的列族添加到指定的数据表
	    admin.addColumn(tableName, columnDescriptor);
	    LOGGER.debug("新建列族 END");
	    result = true;
	} catch (Exception e)
	{
	    result = false;
	    LOGGER.error("新建列族异常!");
	}
	return result;
    }

    /**
     * 删除列族
     * 
     * @param tabName
     * @param fname
     * @return
     * @throws IOException
     */
    public boolean deleteColumnFamily(String tabName, String fname) throws IOException
    {
	Admin admin = connection.getAdmin();
	boolean result = false;
	try
	{
	    LOGGER.debug("删除列族 START");
	    // 取得目标数据表的表名对象
	    TableName tableName = TableName.valueOf(tabName);
	    // 删除指定数据表中的指定列族
	    admin.deleteColumn(tableName, fname.getBytes());
	    LOGGER.debug("删除列族 END");
	} catch (Exception e)
	{
	    LOGGER.error("删除列族异常!");
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
	    LOGGER.debug("插入数据 START");
	    Connection connection = ConnectionFactory.createConnection(config);
	    // 取得一个数据表对象
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 将数据集合插入到数据库
	    table.put(putList);
	    LOGGER.debug("插入数据 END");
	} catch (Exception e)
	{
	    LOGGER.error("插入数据异常!");
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
	    LOGGER.debug("查询整表数据 START");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 取得表中所有数据
	    resultx = table.getScanner(new Scan());
	    // 循环输出表中的数据
	    for (Result result : resultx)
	    {
		byte[] row = result.getRow();
		LOGGER.debug("row key is:" + new String(row));
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
	    LOGGER.debug("查询整表数据 END");
	} catch (Exception e)
	{
	    LOGGER.error("查询整表数据异常!");
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
	    LOGGER.debug("按行键查询表数据 START");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
	    // 新建一个查询对象作为查询条件
	    Get get = new Get(rowKey.getBytes());
	    // 按行键查询数据
	    result = table.get(get);
	    byte[] row = result.getRow();
	    LOGGER.debug("row key is:" + new String(row));
	    List<Cell> listCells = result.listCells();
	    for (Cell cell : listCells)
	    {
		byte[] familyArray = cell.getFamilyArray();
		byte[] qualifierArray = cell.getQualifierArray();
		byte[] valueArray = cell.getValueArray();
		System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray)
			+ new String(valueArray));
	    }
	    LOGGER.debug("按行键查询表数据 END");
	} catch (Exception e)
	{
	    LOGGER.error("按行键查询表数据异常!");
	}
	return result;

    }

    /**
     * 按条件查询表数据
     * 
     * @param tabName
     * @param filter
     * @return
     */
    public ResultScanner queryTableByCondition(String tabName, Filter filter)
    {
	ResultScanner resultScanner = null;
	try
	{
	    LOGGER.debug("按条件查询表数据 START");
	    // 取得数据表对象
	    Connection connection = ConnectionFactory.createConnection(config);
	    Table table = connection.getTable(TableName.valueOf(tabName));
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
		LOGGER.debug("row key is:" + new String(row));
		List<Cell> listCells = result.listCells();
		for (Cell cell : listCells)
		{
		    LOGGER.debug("family:" + Bytes.toString(CellUtil.cloneFamily(cell)));
		    LOGGER.debug("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
		    LOGGER.debug("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
	    }
	    LOGGER.debug("按条件查询表数据 END");
	} catch (Exception e)
	{
	    LOGGER.error("按条件查询表数据异常!");
	}
	return resultScanner;
    }

    /**
     * 列出数据库中所有表
     * 
     * @return
     * @throws IOException
     */
    public HTableDescriptor[] showTables() throws IOException
    {
	Admin admin = connection.getAdmin();
	// 获取数据库中表的集合
	HTableDescriptor[] tableDescriptor = admin.listTables();
	// 遍历打印所有表名
	for (int i = 0; i < tableDescriptor.length; i++)
	{
	    System.out.println(tableDescriptor[i].getNameAsString());
	}
	return tableDescriptor;
    }

}
