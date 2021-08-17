package com.zdjizhi.common;

import com.zdjizhi.utils.HttpClientUtils;
import com.zdjizhi.utils.JsonMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class HttpTest {
    public static void main(String[] args) throws Exception {
        CloseableHttpClient httpclient = HttpClients.custom().build();
        URIBuilder uriBuilder = new URIBuilder("http://192.168.44.3:80");
        HashMap<String, String> parms = new HashMap<>();
        parms.put("password",CommonConfig.BIFANG_SERVER_PASSWORD);
        HttpClientUtils.setUrlWithParams(uriBuilder,CommonConfig.BIFANG_SERVER_ENCRYPTPWD_PATH,parms);
        System.out.println(uriBuilder.toString());

        URI uri = uriBuilder.build();
        System.out.println(HttpClientUtils.httpGet(uri.toString()));

        /*
        URI uri = uriBuilder
                .setPath("/v1/user/encryptpwd")
                .setParameter("password", "admin").build();

        System.out.println(uri.toString());


        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = null;
        try {
            // 执行http get请求
            response = httpclient.execute(httpGet);
            // 判断返回状态是否为200
            if (response.getStatusLine().getStatusCode() == 200) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                System.out.println(content);
                HashMap<String, Object> map = (HashMap<String, Object>) JsonMapper.fromJsonString(content, Object.class);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
*/
    }
}
