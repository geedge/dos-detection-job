package com.zdjizhi.common;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.Executor;


/**
 * @author qidaijie
 * @Package com.zdjizhi
 * @Description:
 * @date 2022/3/1016:58
 */
public class NacosTest {

    /**
     * <dependency>
     * <groupId>com.alibaba.nacos</groupId>
     * <artifactId>nacos-client</artifactId>
     * <version>1.2.0</version>
     * </dependency>
     */

    private static Properties properties = new Properties();
    /**
     * config data id = config name
     */
    private static final String DATA_ID = "dos_baseline.properties";
    /**
     * config group
     */
    private static final String GROUP = "Galaxy";

    private void getProperties() {
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, "192.168.44.12:8848");
        properties.setProperty(PropertyKeyConst.NAMESPACE, "test");
        properties.setProperty(PropertyKeyConst.USERNAME, "nacos");
        properties.setProperty(PropertyKeyConst.PASSWORD, "nacos");
    }


    @Test
    public void GetConfigurationTest() {
        try {
            getProperties();
            ConfigService configService = NacosFactory.createConfigService(properties);
            String content = configService.getConfig(DATA_ID, GROUP, 5000);
            Properties nacosConfigMap = new Properties();
            nacosConfigMap.load(new StringReader(content));
            System.out.println(nacosConfigMap.getProperty("static.sensitivity.threshold"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void ListenerConfigurationTest() {
        getProperties();
        try {
            //first get config
            ConfigService configService = NacosFactory.createConfigService(properties);
            String config = configService.getConfig(DATA_ID, GROUP, 5000);
//            System.out.println(config);

            //start listenner
            configService.addListener(DATA_ID, GROUP, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configMsg) {
                    System.out.println(configMsg);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        //keep running,change nacos config,print new config
        /*
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        */
    }
}
