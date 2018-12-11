package com.hoperun.hbase.facade;

import java.io.IOException;

public interface HBaseUtilService
{
    public void createTable() throws IOException;
    public void insertTest();
    public void tableExists();
}
