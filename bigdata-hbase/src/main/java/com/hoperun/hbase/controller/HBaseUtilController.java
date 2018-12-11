package com.hoperun.hbase.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.hoperun.hbase.facade.HBaseUtilService;


@RestController
@RequestMapping(value = "/hbase")
public class HBaseUtilController
{
    @Autowired
    private HBaseUtilService hBaseUtilService;

    @RequestMapping(value = "/createTable", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public void createTable() throws IOException
    {
	hBaseUtilService.createTable();
    }

    @RequestMapping(value = "/insertTest", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public void insertTest()
    {
	hBaseUtilService.insertTest();
    }

    @RequestMapping(value = "/tableExists", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public void tableExists()
    {
	hBaseUtilService.tableExists();
    }
}