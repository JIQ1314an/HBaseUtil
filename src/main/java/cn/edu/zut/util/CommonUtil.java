package cn.edu.zut.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

/**
 * @Author jiqaun
 * @Date 2021/5/10
 * @TIME 10:42
 */
public class CommonUtil {
    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    public static <T> Map<String, Object> convert(T bean) {
        return convert(bean, true);
    }

    public static <T> Map<String, Object> convert(T bean, boolean isIgnoreNull) {
        Map<String, Object> result = new HashMap<>();
        Class<?> clazz = bean.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            field.setAccessible(true);
            try {
                Object value = field.get(bean); //value可能为null
                if (!isIgnoreNull || value != null) {
                    result.put(field.getName(), String.valueOf(value));
                }
            } catch (IllegalAccessException e) {
                logger.error("访问权限异常", e);
            } catch (Exception e) {
                logger.error("转换异常", e);
            }
        }
        return result;
    }

    /**
     * @param list List<T>
     * @param func Function<T, E>
     * @param <E>
     * @param <T>
     * @return 转换List中类型 T——>E
     */
    public static <E, T> List<E> convert(List<T> list, Function<T, E> func) {
        return list.stream().collect(ArrayList::new, (li, p) -> li.add(func.apply(p)), List::addAll);
    }

    /**
     * 读取配置文件中的key对应的值并添加到list对象里
     *
     * @param key 配置文件中的key
     * @return List<String>
     * @throws NullPointerException
     */
    public static List<String> convert(String key) {
        String valueStr = PropertiesUtil.getProperty(key);
        if (StringUtils.isBlank(valueStr)) logger.error("配置文件中key对应的value值为空", new NullPointerException());

        List<String> list = new LinkedList<>();
        if (valueStr.contains(",")) {
            for (String value : valueStr.split(",")) {
                if (!StringUtils.isBlank(value)) list.add(value.trim());
            }
        } else {
            list.add(valueStr);
        }
        return list;
    }

    public static <T> String getFieldValue(T bean, String fieldName) {
        if (bean == null) return null;
        Class<?> clazz = bean.getClass();
        Object value = null;
        try {
            Field id = clazz.getDeclaredField(fieldName);
            id.setAccessible(true);
            value = id.get(bean);
        } catch (NoSuchFieldException e) {
            logger.error("对象必须有" + fieldName + "字段，但是接收到的对象不具备" + fieldName + "字段", e);

        } catch (IllegalAccessException e) {
            logger.error("字段 " + fieldName + " 不具备访问权限", e);
        }
        return value == null ? null : value.toString();

    }

    public static void close(Connection connection) {
        close(connection, null, null);
    }

    public static void close(Admin admin) {
        close(null, admin, null);
    }

    public static void close(Table table) {
        close(null, null, table);
    }

    public static void close(Connection connection, Admin admin, Table table) {

        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("关闭Table对象异常", e);
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                logger.error("关闭Admin对象异常", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("关闭Connection对象异常", e);
            }
        }

    }
}
