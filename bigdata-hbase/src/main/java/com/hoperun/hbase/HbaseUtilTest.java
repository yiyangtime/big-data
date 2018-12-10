package  com.hoperun.hbase;
 
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseUtilTest {
 
    @Test
    public void createTable(){
        HbaseUtil hu=new HbaseUtil();
        String[] columns=new String[]{"id","name"};
        hu.createTable("info_user",columns);
    }
    
    @Test
    public void insertTest(){
        HbaseUtil hu=new HbaseUtil();
        List<Put> putList = new ArrayList<Put>();
        Put put;
        for(int i = 0; i < 2; i++){
            put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("id"), null, Bytes.toBytes("id" + i));
            put.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("name" + i));
            putList.add(put);
        }
        hu.insert("info_user",putList);
    }
}