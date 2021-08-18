package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;

public class IpUtils {

    /**
     * IP定位库工具类
     */
    public static IpLookup ipLookup = new IpLookup.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
//            .loadDataFileV6(CommonConfig.IP_MMDB_PATH + "ip_v6.mmdb")
//            .loadDataFilePrivateV4(CommonConfig.IP_MMDB_PATH + "ip_private_v4.mmdb")
//            .loadDataFilePrivateV6(CommonConfig.IP_MMDB_PATH + "ip_private_v6.mmdb")
            .build();

    public static void main(String[] args) {
        System.out.println(ipLookup.countryLookup("49.7.115.37"));

//        String ips = "192.168.50.23,192.168.50.45,192.168.56.9,192.168.56.8,192.168.50.58,192.168.56.7,192.168.56.6,192.168.50.40,192.168.50.19,192.168.50.6,192.168.50.4,192.168.56.17,192.168.50.27,192.168.50.26,192.168.50.18,192.168.56.3,192.168.56.10";
//        for (String ip:ips.split(",")){
//            System.out.println(ip+"--"+ipLookup.countryLookup(ip));
//        }

    }



}
