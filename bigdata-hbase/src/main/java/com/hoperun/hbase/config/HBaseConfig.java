package com.hoperun.hbase.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class HBaseConfig
{
    public String zookeeperZnodeParent;
    public String hbaseZookeeperQuorum;
    public String hbaseZookeeperPropertyClientPort;
    public String hbaseMaster;
    
    public String getZookeeperZnodeParent()
    {
        return zookeeperZnodeParent;
    }

    public String getHbaseZookeeperQuorum()
    {
        return hbaseZookeeperQuorum;
    }

    public String getHbaseZookeeperPropertyClientPort()
    {
        return hbaseZookeeperPropertyClientPort;
    }

    public String getHbaseMaster()
    {
        return hbaseMaster;
    }

    @Value("${zookeeper.znode.parent}")
    public void setZookeeperZnodeParent(String zookeeperZnodeParent)
    {
	this.zookeeperZnodeParent = zookeeperZnodeParent;
    }

    @Value("${hbase.zookeeper.quorum}")
    public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum)
    {
	this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    }

    @Value("${hbase.zookeeper.property.clientPort}")
    public void setHbaseZookeeperPropertyClientPort(String hbaseZookeeperPropertyClientPort)
    {
	this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
    }

    @Value("${hbase.master}")
    public void setHbaseMaster(String hbaseMaster)
    {
	this.hbaseMaster = hbaseMaster;
    }

}
