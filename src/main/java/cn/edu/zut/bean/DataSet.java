package cn.edu.zut.bean;


import cn.edu.zut.util.PropertiesUtil;

/**
 * @Author 86131
 * @Date 2021/5/8
 * @TIME 22:21
 */
public class DataSet {

    private static String rowKey;
    private static String family;
    private static String qualifier;
    private static String value;

    static {
        rowKey = PropertiesUtil.getProperty("row_key");
        family = PropertiesUtil.getProperty("family");
        qualifier = PropertiesUtil.getProperty("qualifier");
        value = PropertiesUtil.getProperty("value");
    }

}
