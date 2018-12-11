package com.hoperun.hbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.hoperun.hbase.config.HBaseConfig;

@RestController
@SpringBootApplication
@ServletComponentScan
public class Application extends SpringBootServletInitializer
{

    @Autowired
    private HBaseConfig hBaseConfig;
    
    @RequestMapping(value = "/hbase", method = RequestMethod.GET)
    public String sendNotification()
    {
	return hBaseConfig.getZookeeperZnodeParent();

    }

    public static void main(String[] args)
    {
	System.out.println("==============bigdata-hbase.run start===========");
	SpringApplication.run(Application.class, args);
	System.out.println("==============bigdata-hbase.run end=============");
    }

}
