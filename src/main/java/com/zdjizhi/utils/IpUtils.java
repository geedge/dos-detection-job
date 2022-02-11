package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;

public class IpUtils {

    /**
     * IP定位库工具类
     */
    public static IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4_built_in.mmdb")
            .loadDataFileV6(CommonConfig.IP_MMDB_PATH + "ip_v6_built_in.mmdb")
            .loadDataFilePrivateV4(CommonConfig.IP_MMDB_PATH + "ip_v4_user_defined.mmdb")
            .loadDataFilePrivateV6(CommonConfig.IP_MMDB_PATH + "ip_v6_user_defined.mmdb")
            .build();

    public static void main(String[] args) {
        System.out.println(ipLookup.countryLookup("49.7.115.37"));

        String ips = "182.168.50.23,182.168.50.45,182.168.56.9,182.168.56.8,92.168.50.58,19.168.56.7,12.168.56.6,2.168.50.40,1.168.50.19,9.168.50.6,2.168.50.4,192.168.56.17,192.168.50.27,192.168.50.26,192.168.50.18,192.168.56.3,192.168.56.10";
        for (String ip:ips.split(",")){
            System.out.println(ip+"--"+ipLookup.countryLookup(ip));
        }

    }



}
