package cn.edu.zut.util;

/**
 * @Author jiquan
 * @Date 2021/5/1
 * @TIME 9:05
 * 读取Properties文件的工具类，默认读取resources下所有.properties文件(除log4j.properties外)
 */

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;


public class PropertiesUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    private static final Properties props;

    static {
        File file = new File("src/main/resources");
        props = new Properties();

        for (String fileName : file.list()) {
            if (fileName == "log4j.properties") continue;
            try {
                props.load(new InputStreamReader(Objects.requireNonNull(PropertiesUtil.
                        class.getClassLoader().getResourceAsStream(fileName)), "UTF-8"));
            } catch (IOException e) {
                logger.error("配置文件读取异常", e);
            }
        }


    }

    public static Properties getProperties(String fileName) {
        Properties props = new Properties();
        try {
            props.load(new InputStreamReader(Objects.requireNonNull(PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName)), "UTF-8"));
        } catch (IOException e) {
            logger.error("配置文件读取异常", e);
        }
        return props;
    }

    public static Properties getProperties() {
        return getProperties("hbase.properties");
    }

    public static String getProperty(String key) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return value.trim();
    }

    public static String getProperty(String key, String defaultValue) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value.trim();
    }
}
