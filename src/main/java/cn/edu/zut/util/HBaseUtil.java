package cn.edu.zut.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


/**
 * @Author 86131
 * @Date 2021/5/10
 * @TIME 11:07
 */
public class HBaseUtil {
    private  static Connection conn;
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", PropertiesUtil1.getProperty("zookeeper_connect"));
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  static  <T> boolean saveBean(String tableName,String rowKey,String family,T bean){
        boolean status = false;
        Table table = null;
        try {
             table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            Map<String, Object> datas = CommonUtil.convert(bean);
            for(Map.Entry<String , Object> data : datas.entrySet()){
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(data.getKey()), Bytes.toBytes(data.getValue().toString()));
            }
             table.put(put);
             status = true;
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table!= null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return  status;
    }
    public  static<T> T  get(String tableName, String rowKey,String family,Class<T> beanType) throws Exception {
        Table table = null;
        T bean = null;
        try {
            bean = beanType.newInstance();
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new  Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
            Map<String, Object> data = new HashMap<>();
            familyMap.forEach((key, value)->{
                data.put(Bytes.toString(key), Bytes.toString(value));
            });

            BeanUtils.populate(bean,data);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table!= null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return  bean;

    }


    public static void main(String[] args) throws IOException {
      Admin admin =  conn.getAdmin();
    }
}
