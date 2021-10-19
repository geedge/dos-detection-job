package com.zdjizhi.etl;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosDetectionThreshold;
import com.zdjizhi.utils.HttpClientUtils;
import com.zdjizhi.utils.JsonMapper;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wlh
 */
public class ParseStaticThreshold {
    private static Logger logger = LoggerFactory.getLogger(ParseStaticThreshold.class);
    private static String encryptpwd;

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    private static JavaType thresholdType = jsonMapperInstance.createCollectionType(ArrayList.class, DosDetectionThreshold.class);

    static {
        //加载加密登录密码
        encryptpwd = getEncryptpwd();
    }

    /**
     * 获取加密密码
     */
    private static String getEncryptpwd() {
        String psw = HttpClientUtils.ERROR_MESSAGE;
        try {
            URIBuilder uriBuilder = new URIBuilder(CommonConfig.BIFANG_SERVER_URI);
            HashMap<String, Object> parms = new HashMap<>();
            parms.put("password", "admin");
            HttpClientUtils.setUrlWithParams(uriBuilder, CommonConfig.BIFANG_SERVER_ENCRYPTPWD_PATH, parms);
            String resposeJsonStr = HttpClientUtils.httpGet(uriBuilder.build());
            if (!HttpClientUtils.ERROR_MESSAGE.equals(resposeJsonStr)) {
                HashMap<String, Object> resposeMap = jsonMapperInstance.fromJson(resposeJsonStr, hashmapJsonType);
                boolean success = (boolean) resposeMap.get("success");
                String msg = resposeMap.get("msg").toString();
                if (success) {
                    HashMap<String, Object> data = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(resposeMap.get("data")), hashmapJsonType);
                    psw = data.get("encryptpwd").toString();
                } else {
                    logger.error(msg);
                }
            }
        } catch (URISyntaxException e) {
            logger.error("构造URI异常", e);
        } catch (Exception e) {
            logger.error("获取encryptpwd失败", e);
        }
        return psw;
    }

    /**
     * 登录bifang服务，获取token
     *
     * @return token
     */
    private static String loginBifangServer() {
        String token = HttpClientUtils.ERROR_MESSAGE;
        try {
            if (!HttpClientUtils.ERROR_MESSAGE.equals(encryptpwd)) {
                URIBuilder uriBuilder = new URIBuilder(CommonConfig.BIFANG_SERVER_URI);
                HashMap<String, Object> parms = new HashMap<>();
                parms.put("username", "admin");
                parms.put("password", encryptpwd);
                HttpClientUtils.setUrlWithParams(uriBuilder, CommonConfig.BIFANG_SERVER_LOGIN_PATH, parms);
                String resposeJsonStr = HttpClientUtils.httpPost(uriBuilder.build(), null);
                if (!HttpClientUtils.ERROR_MESSAGE.equals(resposeJsonStr)) {
                    HashMap<String, Object> resposeMap = jsonMapperInstance.fromJson(resposeJsonStr, hashmapJsonType);
                    boolean success = (boolean) resposeMap.get("success");
                    String msg = resposeMap.get("msg").toString();
                    if (success) {
                        HashMap<String, Object> data = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(resposeMap.get("data")), hashmapJsonType);
                        token = data.get("token").toString();
                    } else {
                        logger.error(msg);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("登录失败,未获取到token ", e);
        }
        return token;
    }

    /**
     * 获取静态阈值配置列表
     *
     * @return thresholds
     */
    private static ArrayList<DosDetectionThreshold> getDosDetectionThreshold() {
        ArrayList<DosDetectionThreshold> thresholds = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(CommonConfig.BIFANG_SERVER_URI);
            HashMap<String, Object> parms = new HashMap<>();
            parms.put("pageSize", -1);
            parms.put("orderBy", "profileId asc");
            parms.put("isValid", 1);
            HttpClientUtils.setUrlWithParams(uriBuilder, CommonConfig.BIFANG_SERVER_POLICY_THRESHOLD_PATH, parms);
            String token = CommonConfig.BIFANG_SERVER_TOKEN;
            if (!HttpClientUtils.ERROR_MESSAGE.equals(token)) {
                BasicHeader authorization = new BasicHeader("Authorization", token);
                BasicHeader authorization1 = new BasicHeader("Content-Type", "application/x-www-form-urlencoded");
                String resposeJsonStr = HttpClientUtils.httpGet(uriBuilder.build(), authorization, authorization1);
                if (!HttpClientUtils.ERROR_MESSAGE.equals(resposeJsonStr)) {
                    HashMap<String, Object> resposeMap = jsonMapperInstance.fromJson(resposeJsonStr, hashmapJsonType);
                    boolean success = (boolean) resposeMap.get("success");
                    String msg = resposeMap.get("msg").toString();
                    if (success) {
                        HashMap<String, Object> data = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(resposeMap.get("data")), hashmapJsonType);
                        thresholds = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(data.get("list")), thresholdType);
                        logger.info("获取到静态阈值配置{}条", thresholds.size());
                    } else {
                        logger.error(msg);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("获取静态阈值配置失败,请检查bifang服务或登录配置信息 ", e);
        }
        return thresholds;
    }

    /**
     * 基于静态阈值构建threshold RangeMap，k:IP段或具体IP，v:配置信息
     *
     * @return threshold RangeMap
     */
    static TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> createStaticThreshold() {
        TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> thresholdRangeMap = TreeRangeMap.create();
        try {
            ArrayList<DosDetectionThreshold> dosDetectionThreshold = getDosDetectionThreshold();
            if (dosDetectionThreshold != null && !dosDetectionThreshold.isEmpty()) {
                for (DosDetectionThreshold threshold : dosDetectionThreshold) {
                    ArrayList<String> serverIpList = threshold.getServerIpList();
                    for (String sip : serverIpList) {
                        IPAddressString ipAddressString = new IPAddressString(sip);
                        if (ipAddressString.isIPAddress()) {
                            IPAddress address = ipAddressString.getAddress();
                            Map<String, DosDetectionThreshold> floodTypeThresholdMap = new HashMap<>();
                            floodTypeThresholdMap.put(threshold.getAttackType(), threshold);
                            if (address.isPrefixed()) {
                                IPAddress lower = address.getLower();
                                IPAddress upper = address.getUpper();
                                if (!address.isMultiple()) {
                                    lower = address.adjustPrefixLength(address.getBitCount());
                                    upper = address.toMaxHost().withoutPrefixLength();
                                }
                                Map.Entry<Range<IPAddress>, Map<String, DosDetectionThreshold>> lowerEntry = thresholdRangeMap.getEntry(lower);
                                Map.Entry<Range<IPAddress>, Map<String, DosDetectionThreshold>> upperEntry = thresholdRangeMap.getEntry(upper);
                                if (lowerEntry != null && upperEntry == null) {
                                    Range<IPAddress> lowerEntryKey = lowerEntry.getKey();
                                    Map<String, DosDetectionThreshold> lowerEntryValue = lowerEntry.getValue();
                                    lowerEntryValue.put(threshold.getAttackType(), threshold);
                                    thresholdRangeMap.put(Range.closedOpen(lowerEntryKey.lowerEndpoint(), lower), lowerEntryValue);
                                    thresholdRangeMap.put(Range.closed(lower, upper), floodTypeThresholdMap);
                                } else if (lowerEntry == null && upperEntry != null) {
                                    Range<IPAddress> upperEntryKey = upperEntry.getKey();
                                    Map<String, DosDetectionThreshold> upperEntryValue = upperEntry.getValue();
                                    upperEntryValue.put(threshold.getAttackType(), threshold);
                                    thresholdRangeMap.put(Range.openClosed(upper, upperEntryKey.upperEndpoint()), upperEntryValue);
                                    thresholdRangeMap.put(Range.closed(lower, upper), floodTypeThresholdMap);
                                } else {
                                    thresholdRangeMap.put(Range.closed(lower, upper), floodTypeThresholdMap);
                                }
                            } else {
                                Map.Entry<Range<IPAddress>, Map<String, DosDetectionThreshold>> entry = thresholdRangeMap.getEntry(address);
                                if (entry != null) {
                                    Range<IPAddress> entryKey = entry.getKey();
                                    Map<String, DosDetectionThreshold> entryValue = entry.getValue();
                                    if (entryKey.lowerEndpoint() == entryKey.upperEndpoint()) {
                                        entryValue.put(threshold.getAttackType(), threshold);
                                        thresholdRangeMap.put(Range.closed(address, address), entryValue);
                                    } else {
                                        thresholdRangeMap.put(Range.closed(address, address), floodTypeThresholdMap);
                                    }
                                } else {
                                    thresholdRangeMap.put(Range.closed(address, address), floodTypeThresholdMap);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("构建threshold RangeMap失败", e);
        }
        return thresholdRangeMap;
    }

    public static void main(String[] args) {

        ArrayList<DosDetectionThreshold> dosDetectionThreshold = getDosDetectionThreshold();
        dosDetectionThreshold.forEach(System.out::println);


        System.out.println("------------------------");
        TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> staticThreshold = createStaticThreshold();

        System.out.println("------------------------");
        Map<Range<IPAddress>, Map<String, DosDetectionThreshold>> rangeMapMap = staticThreshold.asMapOfRanges();
        for (Range<IPAddress> range : rangeMapMap.keySet()) {
            Map<String, DosDetectionThreshold> thresholdMap = rangeMapMap.get(range);
            for (String type : thresholdMap.keySet()) {
                DosDetectionThreshold threshold = thresholdMap.get(type);
                System.out.println(range + "---" + type + "---" + threshold);
            }
        }


    }


}
