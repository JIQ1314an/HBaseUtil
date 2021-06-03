package cn.edu.zut.util;


import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author jiquan
 * @Date 2021/5/1
 * @TIME 9:32
 */
public class Demo {
    Logger logger = LoggerFactory.getLogger(Demo.class);

    public static void main(String[] args) throws IOException {
//        Properties properties = PropertiesUtil.getProperties();
//        String value = properties.getProperty("name");
        String value = PropertiesUtil.getProperty("value");
        System.out.println(value);

//        File file =new File("src/main/resources");
//        System.out.println(file.list());
//        SuffixFileFilter sufFilter = new SuffixFileFilter(".txt");
//        PrefixFileFilter preFilter = new PrefixFileFilter("log4j");
//        Arrays.stream(file.list(preFilter)).forEach(System.out::println);
    }

    @Test
    public void testGetConn() {
        Connection conn = HBaseOptUtil.getConnection();
        System.out.println(conn);
    }

    @Test
    public void testGetAdmin() {
        HBaseOptUtil.getConnection(); //初始化连接
        Admin admin = HBaseOptUtil.getAdmin();
        System.out.println(admin);
    }

    @Test
    public void testGetTable() {
        HBaseOptUtil.getConnection(); //初始化连接
        Table table = HBaseOptUtil.getTable();
        System.out.println(table);
    }

    @Test
    public void testIsTableExist() {
        boolean flag = HBaseOptUtil.isTableExist();
        System.out.println(flag);
        System.out.println(HBaseOptUtil.isTableExist("student_test"));
    }

    @Test
    public void testCreateTab() {
        boolean flag = HBaseOptUtil.createTable();
        System.out.println(flag);
    }

    @Test
    public void testDropTab() {
        boolean flag = HBaseOptUtil.dropTab();
        System.out.println(flag);
    }


    @Test
    public void testAddFam() {
        boolean flag = HBaseOptUtil.addFamily();
        System.out.println(flag);
    }

    @Test
    public void testDelFam() {
        boolean flag = HBaseOptUtil.delFamilies();
        System.out.println(flag);
    }

    @Test
    public void testModTabName() {
        boolean flag = HBaseOptUtil.modTabName("student", "newStu");
        System.out.println(flag);
    }

    @Test
    public void testSingleInsert() {
        boolean flag = HBaseOptUtil.singleInsert();
        System.out.println(flag);
    }

    // 反射测试
    @Test
    public void testReflect() throws Exception {
        Class<?> c = Class.forName("cn.edu.zut.bean.DataSet");
        Object obj = c.newInstance();
        System.out.println(c.getSimpleName());
        Field f = c.getDeclaredField("rowKey");
        f.setAccessible(true); //必须有
        System.out.println(f.get(obj));
        Field[] fs = c.getDeclaredFields();
        for (Field f1 : fs) {
            System.out.println(f1.getName());
        }
    }

    // 反射测试
    @Test
    public void testBean() throws Exception {
        Class<?> c = Class.forName("cn.edu.zut.bean.DataSet");
        Object obj = c.newInstance();
        Field f = c.getDeclaredField("values");
        f.setAccessible(true);
        Object[] a = (Object[]) f.get(obj);
        Arrays.stream(a).forEach(System.out::println);
    }

    @Test
    public void show() {
        List<String> a = new ArrayList();
        a.add("1");
        a.add("2");
        a.forEach(b -> System.out.println(b));
    }

    public String demo(String... str) {
        return null;
    }
}
