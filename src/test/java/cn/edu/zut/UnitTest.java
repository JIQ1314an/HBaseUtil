package cn.edu.zut;

import cn.edu.zut.bean.DataSet;
import cn.edu.zut.bean.Student;
import cn.edu.zut.bean.Worker;
import cn.edu.zut.util.HBaseOptUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author jiquan
 * @Date 2021/6/3
 * @TIME 10:28
 */
public class UnitTest {
    Logger logger = LoggerFactory.getLogger(cn.edu.zut.UnitTest.class);

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
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.isTableExist();
        System.out.println(flag);
        System.out.println(HBaseOptUtil.isTableExist("student_tests"));
    }

    @Test
    public void testCreateTab() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.createTable();
        System.out.println(flag);
    }

    @Test
    public void testDropTab() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.dropTab();
        System.out.println(flag);
    }


    @Test
    public void testAddFam() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.addFamily();
        System.out.println(flag);
    }

    @Test
    public void testDelFam() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.delFamilies();
        System.out.println(flag);
    }

    @Test
    public void testModTabName() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.modTabName("student_test", "student");
        System.out.println(flag);
    }


    // 反射测试
    @Test
    public void testBeanReflect() throws Exception {
//        Class<?> c = Class.forName("cn.edu.zut.bean.DataSet");
        Class<?> c = DataSet.class;
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

    @Test
    public void testSingleInsert() {
        HBaseOptUtil.getConnection();//初始化连接
        boolean flag = HBaseOptUtil.singleInsert();
        System.out.println(flag);
    }

    @Test
    public void testPutByRowKey() {
        HBaseOptUtil.getConnection();//初始化连接
         /*1.方法一：传入一个Bean对象
        Student stu = new Student("zhangsan", 20, "13140470218");
        // 任意一个Bean对象皆可以传入
        boolean flag = HBaseOptUtil.putByRowKey("test", "100", "info", stu);*/

        /* 2. 方法二：传入列名，列值
        boolean flag = HBaseOptUtil.putByRowKey("newStu", "1003", "info",
                "email", "jiq1314an@gmail.com");*/

        // 3.方法三：传入一个map对象
        Map<String, Object> map = new HashMap<>();
        map.put("gender", 1); // 0表女,1表男
        map.put("nickName", "addis");
        map.put("qq", "1314520");

        boolean flag = HBaseOptUtil.putByRowKey("newStu", "1003", "info", map);
        System.out.println(flag);
    }

    @Test
    public void testPut() {
        HBaseOptUtil.getConnection();//初始化连接
        Worker worker = new Worker("1004", "lihua", 1, 30, "12345678");
        // 任意一个Bean对象皆可以传入
        boolean flag = HBaseOptUtil.put("newStu", "info", worker);
        System.out.println(flag);
    }

    @Test
    public void testPuts() {
        HBaseOptUtil.getConnection();//初始化连接
        List<Worker> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String id = OptUtil.fill(i, 2);
            String name = "mazi" + i;
            int gender = OptUtil.getRandom(1, 100) % 2 == 0 ? 1 : 0;
            int age = OptUtil.getRandom(15, 50);
            String phone = OptUtil.getPhone();
            Worker worker = new Worker(id, name, gender, age, phone);
            list.add(worker);
        }
        boolean flag = HBaseOptUtil.puts("test", "info", list);
        System.out.println(flag);
    }

    @Test
    public void testGetBeanByRowKey() {
        HBaseOptUtil.getConnection();//初始化连接
        Worker worker = HBaseOptUtil.getBeanByRowKey("test", "99", "info", Worker.class);
        System.out.println(worker);
        Student stu = HBaseOptUtil.getBeanByRowKey("test", "100", "info", Student.class);
        System.out.println(stu);
    }

}
