package cn.edu.zut.util;


import org.apache.commons.beanutils.ConvertUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @Author jiquan
 * @Date 2021/5/1
 * @TIME 9:14
 */
public class HBaseOptUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseOptUtil.class);
    private static String quorum;
    private static Connection conn;
    private static boolean flag;
    private static Admin admin;
    private static String tableName;

    static {
        quorum = PropertiesUtil.getProperty("hbase_zookeeper_quorum");
    }

    /**
     * 获取HBase连接对象
     * 参数默认来自hbase.properties的quorum对应的值
     * 默认连接
     *
     * @return Connection
     */
    public static Connection getConnection() {
        return getConnection(quorum);
    }

    public static Connection getConnection(String quorum) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error("获取HBase连接对象失败，配置信息有误", e);

        }
        return conn;
    }

    public static Connection setConnection(Connection connection) {
        return conn = connection;
    }

    /**
     * 获取Admin对象
     *
     * @return Admin
     */
    public static Admin getAdmin() {
        if (check()) {
            try {
                return conn.getAdmin();
            } catch (IOException e) {
                logger.error("获取Admin对象失败", e);
            }
        }
        return null;
    }

    /**
     * 获取Table对象
     * 参数默认是hbase.properties里key为table_name对应的值
     * 表即便不存在，也会创建表对象
     *
     * @return Table
     */
    public static Table getTable() {
        String tableName = PropertiesUtil.getProperty("table_name");
        return getTable(tableName);
    }

    public static Table getTable(String tableName) {
        if (check()) {
            try {
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("获取Table对象失败", e);
            }
        }
        return null;
    }

    /**
     * 创建表
     * 表名默认读取配置文件里的
     * minVersion、minVersion都是1
     *
     * @return boolean 成功为true,失败为false,表已存在也返回false
     */
    public static boolean createTable() {
        tableName = PropertiesUtil.getProperty("table_name");
        return createTable(tableName, CommonUtil.convert("column_families"), 1, 1);
    }

    public static boolean createTable(String tableName, String family, int minVersion, int maxVersion) {
        List<String> families = new LinkedList();
        families.add(family);
        return createTable(tableName, families, minVersion, maxVersion);
    }

    public static boolean createTable(String tableName, String family, int version) {
        return createTable(tableName, family, version, version);
    }

    public static boolean createTable(String tableName, String family) {
        return createTable(tableName, family, 1, 1);
    }

    public static boolean createTable(String tableName, int minVersion, int maxVersion, String... family) {
        List<String> families = Arrays.asList(family);
        return createTable(tableName, families, minVersion, maxVersion);
    }

    public static boolean createTable(String tableName, int version, String... family) {
        return createTable(tableName, version, version, family);
    }

    public static boolean createTable(String tableName, String... family) {
        return createTable(tableName, 1, 1, family);
    }

    public static boolean createTable(String tableName, List<String> families, int minVersion, int maxVersion) {
        if (isTableExist(tableName)) {
            logger.warn("表已经存在，创建表失败");
            return false; //表存在, 全局变量flag设置为true,所以此处也可以使用!flag
        }
        if (families == null || families.isEmpty()) {
            logger.error("列族数据为空，创建表失败");
            return false;
        }
        admin = getAdmin();
        try {
            checkVersion(minVersion, maxVersion);
            if (admin != null) {
                List<ColumnFamilyDescriptor> colFamilies = CommonUtil.convert(families, family -> ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(family))
                        .setMinVersions(minVersion)
                        .setMaxVersions(maxVersion)
                        .build());
                TableDescriptor tabDes = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamilies(colFamilies).build();

                admin.createTable(tabDes);
                flag = true;
            }
        } catch (IOException e) {
            logger.error("创建表操作异常", e);
        } catch (Exception e) {
            logger.error("版本数参数异常", e);
        } finally {
            CommonUtil.close(admin);
        }
        return flag;
    }


    /**
     * 默认参数读取add_column_families和table_name
     *
     * @return boolean
     */
    public static boolean addFamily() {
        return addFamily(PropertiesUtil.getProperty("table_name"), PropertiesUtil.getProperty("add_column_family"));
    }

    public static boolean addFamily(String tableName, String columnFamily) {
        if (!isTableExist(tableName)) {
            logger.warn("表不存在,添加列失败");
            return false;
        }
        admin = getAdmin();
        ColumnFamilyDescriptor colFamDes = ColumnFamilyDescriptorBuilder.of(columnFamily);
        try {
            admin.addColumnFamily(TableName.valueOf(tableName), colFamDes);
            flag = true;
        } catch (IOException e) {
            logger.error("添加列族异常", e);
        } finally {
            CommonUtil.close(admin);
        }
        return flag;
    }

    /**
     * 删除列族
     * 至少保留一个列族，不然会报错
     * 默认读取配置文件中的参数：del_column_families和table_name
     *
     * @return boolean
     */
    public static boolean delFamilies() {
        return delFamilies(PropertiesUtil.getProperty("table_name"), CommonUtil.convert("del_column_families"));
    }

    public static boolean delFamilies(String tableName, String family) {
        List<String> list = new ArrayList<>();
        list.add(family);
        return delFamilies(tableName, list);
    }

    public static boolean delFamilies(String tableName, String... family) {
        List<String> columnFamilies = Arrays.asList(family);
        return delFamilies(tableName, columnFamilies);
    }

    public static boolean delFamilies(String tableName, List<String> families) {
        if (!isTableExist(tableName)) {
            logger.warn("表不存在,删除列失败");
            return false;
        }
        //表要是存在，全局flag为被赋值为true,所以此处flag的值需要设为false
        flag = false;
        admin = getAdmin();
        try {
            for (String family : families) {
                admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(family));
            }
            flag = true;
        } catch (IOException e) {
            logger.error("删除列异常", e);
        } finally {
            CommonUtil.close(admin);
        }
        return flag;
    }

    /**
     * 修改表名
     * 配置文件中的table_name为旧表名，modified_table_name的值为新的表名
     *
     * @return boolean
     */
    public static boolean modTabName() {
        String oldTabName = PropertiesUtil.getProperty("table_name");
        String newTabName = PropertiesUtil.getProperty("modified_table_name");
        return modTabName(oldTabName, newTabName);
    }

    public static boolean modTabName(String oldTabName, String newTabName) {
        if (!isTableExist(oldTabName)) {
            logger.warn("表不存在,修改表名失败");
            return false;
        }
        flag = false;
        String snapshotName = oldTabName + "_snap";
        admin = getAdmin();
        try {
            //做快照之前先禁用表
            admin.disableTable(TableName.valueOf(oldTabName));
            //把之前的表做个快照
            admin.snapshot(snapshotName, TableName.valueOf(oldTabName));
            //然后在从当前快照中克隆出一个新的表
            admin.cloneSnapshot(snapshotName, TableName.valueOf(newTabName));
            //删除快照
            admin.deleteSnapshot(snapshotName);
            //删除之前的表
            admin.deleteTable(TableName.valueOf(oldTabName));
            flag = true;
        } catch (IOException e) {
            logger.error("修改表名异常", e);
        } finally {
            CommonUtil.close(admin);
        }
        return flag;
    }

    /**
     * 判断表是否存在
     * 默认读取配置文件中的表名并进行判断
     *
     * @return boolean 存在为true,不存在为false
     */
    public static boolean isTableExist() {
        tableName = PropertiesUtil.getProperty("table_name");
        return isTableExist(tableName);
    }

    public static boolean isTableExist(String tableName) {
        flag = false;
        if (check()) {
            admin = getAdmin();
            try {
                flag = admin.tableExists(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("判断异常", e);
            } finally {
                CommonUtil.close(admin);
            }
        }
        return flag;
    }

    /**
     * 删除表
     *
     * @return boolean 删除成功返回true,删除失败返回false
     */
    public static boolean dropTab() {
        tableName = PropertiesUtil.getProperty("table_name");
        return dropTab(tableName);
    }

    public static boolean dropTab(String tableName) {
        if (!isTableExist(tableName)) {
            logger.warn("表不存在,删除表失败");
            return false;
        }
        flag = false;
        admin = getAdmin();
        try {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            flag = true;
        } catch (IOException e) {
            logger.error("删除失败", e);
        }
        return flag;
    }

    public static boolean putByRowKey(String tableName, String rowKey, String family, Map<String, Object> map) {
        if (!isTableExist(tableName)) {
            logger.warn("表不存在,put失败");
            return false;
        }
        if (map == null || map.isEmpty()) {
            logger.warn("数据为空,未添加任何数据");
            return false;
        }
        Table table = getTable(tableName);
        flag = false;
        if (table != null) {
            Put put = new Put(Bytes.toBytes(rowKey));
            // value == null ? "null" : value.toString() --> String.valueOf(value)
            //map的key是列名，value是列值，使用map的好处是k是唯一的
            map.forEach((key, value) -> {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(String.valueOf(value)));
            });
            try {
                table.put(put);
                flag = true;
            } catch (IOException e) {
                logger.error("插入数据异常", e);
            } finally {
                CommonUtil.close(table);
            }
        }
        return flag;

    }

    public static boolean putByRowKey(String tableName, String rowKey, String family, String column, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(column, value);
        return putByRowKey(tableName, rowKey, family, map);
    }

    public static <T> boolean putByRowKey(String tableName, String rowKey, String family, T bean) {
        return putByRowKey(tableName, rowKey, family, CommonUtil.convert(bean, true));
    }

    /**
     * 以bean对象字段中的id作为rowKey
     *
     * @param tableName
     * @param family
     * @param bean      必须有id这个字段，且值不能为null
     * @param <T>
     * @return boolean
     */
    public static <T> boolean put(String tableName, String family, T bean) {
        String rowKey = CommonUtil.getFieldValue(bean, "id");
        if (rowKey == null) {
            logger.error("对象的id字段数据为空，添加数据失败");
            return false;
        }
        return putByRowKey(tableName, rowKey, family, CommonUtil.convert(bean));
    }

    public static <T> boolean puts(String tableName, String family, List<T> beans) {
        if (beans == null || beans.isEmpty()) {
            logger.warn("数据为空，未添加任何数据");
            return false;
        }
        flag = false;
        Table table = getTable(tableName);
        List<Put> puts = new ArrayList<>();
        beans.forEach(bean -> puts.add(convertToPut(bean, family)));
        if (table != null) {
            try {
                table.put(puts);
            } catch (IOException e) {
                logger.error("插入数据异常", e);
            } finally {
                CommonUtil.close(table);
            }
            flag = true;
        }
        return flag;
    }

    /**
     * 通过rowKey查询到数据并封装成bean对象，
     * 所以必须要知道bean的类型
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param beanType
     * @param <T>
     * @return
     */
    public static <T> T getBeanByRowKey(String tableName, String rowKey, String family, Class<T> beanType) {
        //反射的特点，可以通过字段或者构造器去创建对象
        Table table = getTable(tableName);
        if (table == null) return null;
        T t = null;
        try {
            Constructor<T> constructor = beanType.getConstructor();
            t = constructor.newInstance();
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                //key为列名
                Field decField = beanType.getDeclaredField(Bytes.toString(entry.getKey()));
                decField.setAccessible(true);
                //set方法，给t创建字段 (类型（decField.getType） 字段名（decField.getName） = 字段值(decField.get(obj)))
                decField.set(t, ConvertUtils.convert(Bytes.toString(entry.getValue()), decField.getType()));
            }
        } catch (NoSuchMethodException e) {
            logger.error("BeanType必须具备无参构造器, 而当前对象不具备无参构造器", e);
        } catch (InstantiationException e) {
            logger.error("实例化Bean对象异常", e);
        } catch (IllegalAccessException e) {
            logger.error("访问权限异常", e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("根据 RowKey 获取数据失败", e);
        } catch (NoSuchFieldException e) {
            logger.error("该字段不存在", e);
        } finally {
            CommonUtil.close(table);
        }
        return t;
    }

    /**
     * 实现通过传入对象的方式添加数据【需要利用反射机制】
     *
     * @return boolean
     */
    public static boolean singleInsert() {
        try {
            return singleInsert(Class.forName("cn.edu.zut.bean.DataSet"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 实现通过传入对象的方式添加数据【需要利用反射机制】
     *
     * @param c 字节码对象
     * @return boolean
     */
    public static boolean singleInsert(Class<?> c) {
        Field[] decFields = c.getDeclaredFields();
        Map<String, String> map = new HashMap<>();
        try {
            for (Field decF : decFields) {
                decF.setAccessible(true);
                map.put(decF.getName(), String.valueOf(decF.get(c.newInstance())));
            }
            //1.创建Put对象，并设置 RowKey
            Put put = new Put(Bytes.toBytes(map.get("rowKey")));
            //2.增加列和数据【参数分别为：列族名family，列名qualifier，列的数据value】
            put.addColumn(Bytes.toBytes(map.get("family")), Bytes.toBytes(map.get("qualifier")), Bytes.toBytes(map.get("value")));

            Table table = getTable(PropertiesUtil.getProperty("table_name"));
            table.put(put);
            flag = true;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 检查是否连接
     *
     * @return
     */
    private static boolean check() {
        if (conn == null) {
            logger.error("创建HBase连接失败，请重新配置HBase的Zookeeper地址");
            return false;
        }
        return true;
    }

    /**
     * 检查输入的版本数是否合法
     *
     * @param minVersion
     * @param maxVersion
     * @throws IllegalArgumentException
     */
    private static void checkVersion(int minVersion, int maxVersion) throws IllegalArgumentException {
        if (minVersion > maxVersion) {
            throw new IllegalArgumentException(maxVersion + " 必须大于等于 " + minVersion);
        }
        if (minVersion < 1) {
            throw new IllegalArgumentException(minVersion + " 必须大于等于 1");
        }

    }

    private static <T> Put convertToPut(T bean, String family) {
        Put put = new Put(Bytes.toBytes(CommonUtil.getFieldValue(bean, "id")));
        //map的key是列名，value是列值 (其中，列名id及其对应值也会添加进去)
        CommonUtil.convert(bean, true).forEach((key, value) -> put.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(key),
                Bytes.toBytes(String.valueOf(value))
        ));
        return put;
    }
}
