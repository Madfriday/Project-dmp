package Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringUtils;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


public class Extract4Busness {

    public  static String getBuss(String latlong) throws Exception{

        String business = null;



        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("ak", "ZwVpa4ZjK0j6q0DeKUknEKCGF4bVZTB5");
        paramsMap.put("output", "json");
        paramsMap.put("coordtype", "wgs84ll");
        paramsMap.put("location", latlong);



        String paramsStr = toQueryString(paramsMap);
//        System.out.println(paramsStr);
//        System.out.println("");


       HttpClient hp =  new HttpClient();
       GetMethod getmethod = new GetMethod("http://api.map.baidu.com/reverse_geocoding/v3/?" + paramsStr);
       int code = hp.executeMethod(getmethod);

       if(code == 200){
           String as = getmethod.getResponseBodyAsString();
//           getmethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET,"utf-8");
           getmethod.releaseConnection();

           JSONObject json = JSON.parseObject(as);
           JSONObject result = json.getJSONObject("result");
           business = result.getString("business");

           if(StringUtils.isEmpty(business)){
               JSONArray pois = json.getJSONArray("pois");
               if(pois.size() > 0){
                  business = pois.getJSONObject(0).getString("tag");

               }
           }

       }
//        System.out.println(business);
       return business;
    }





    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }
}