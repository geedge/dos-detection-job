package com.zdjizhi.utils;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.Executor;

public class NacosUtils {
    private static final Logger logger = LoggerFactory.getLogger(NacosUtils.class);
    private static Properties nacosProperties = new Properties();
    private static Properties commonProperties = new Properties();


    private static final String NACOS_SERVER_ADDR = CommonConfigurations.getStringProperty("nacos.server.addr");
    private static final String NACOS_NAMESPACE = CommonConfigurations.getStringProperty("nacos.namespace");
    private static final String NACOS_USERNAME = CommonConfigurations.getStringProperty("nacos.username");
    private static final String NACOS_PASSWORD = CommonConfigurations.getStringProperty("nacos.password");
    private static final String NACOS_DATA_ID = CommonConfigurations.getStringProperty("nacos.data.id");
    private static final String NACOS_GROUP = CommonConfigurations.getStringProperty("nacos.group");
    private static final long NACOS_READ_TIMEOUT = CommonConfigurations.getLongProperty("nacos.read.timeout");

    static {
        createConfigService();
    }

    private static void getProperties() {
        nacosProperties.setProperty(PropertyKeyConst.SERVER_ADDR, NACOS_SERVER_ADDR);
        nacosProperties.setProperty(PropertyKeyConst.NAMESPACE, NACOS_NAMESPACE);
        nacosProperties.setProperty(PropertyKeyConst.USERNAME, NACOS_USERNAME);
        nacosProperties.setProperty(PropertyKeyConst.PASSWORD, NACOS_PASSWORD);
    }

    private static void createConfigService() {
        try {
            getProperties();
            ConfigService configService = NacosFactory.createConfigService(nacosProperties);
            String config = configService.getConfig(NACOS_DATA_ID, NACOS_GROUP, NACOS_READ_TIMEOUT);
            commonProperties.load(new StringReader(config));

            configService.addListener(NACOS_DATA_ID, NACOS_GROUP, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configMsg) {
                    try {
                        commonProperties.clear();
                        commonProperties.load(new StringReader(configMsg));
                    } catch (IOException e) {
                        logger.error("监听nacos配置失败", e);
                    }
                    System.out.println(configMsg);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("获取nacos配置失败", e);
        }
    }

    public static String getStringProperty(String key) {
        return commonProperties.getProperty(key);
    }

    public static Integer getIntProperty(String key) {
        return Integer.parseInt(commonProperties.getProperty(key));
    }

    public static Double getDoubleProperty(String key) {
        return Double.parseDouble(commonProperties.getProperty(key));
    }

    public static Long getLongProperty(String key) {
        return Long.parseLong(commonProperties.getProperty(key));
    }

    public static Boolean getBooleanProperty(String key) {
        return "true".equals(commonProperties.getProperty(key).toLowerCase().trim());
    }

}
