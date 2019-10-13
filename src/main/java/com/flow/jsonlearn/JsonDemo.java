/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/12
 * Time: 16:28
 * To change this template use File | Settings | File Templates.
 * Description:  json学习
 **/
package com.flow.jsonlearn;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.deserializer.JSONPDeserializer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class JsonDemo {
    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        /*
         *   编码过程，put<key,value> = <String,Object>
         * */
        jsonObject.put("string", "string");
        jsonObject.put("int", 5);
        jsonObject.put("float", 21);
        List<String> list = new LinkedList<String>();
        list.add("first");
        list.add("second");
        jsonObject.put("list", list);
        jsonObject.put("null", null);
        System.out.println(jsonObject);

        /*
         *   解码过程
         * */
        String str = jsonObject.getString("string");
        int testInt = jsonObject.getIntValue("int");
        System.out.println(str + "  " + testInt);

        List<String> list1 = (List<String>) jsonObject.get("list");
        System.out.println(list);
        /*
         *   将JSON转化为字符串
         * */
        String jsonString = jsonObject.toJSONString();
        System.out.println(jsonString);

        /*
         *   从字符串解析JSON
         * */
        JSONObject parseJSON = JSON.parseObject(jsonString);
        System.out.println(parseJSON);

        /*
         *   将对象解析成JSON字符串
         * */
        String objectJSON = JSON.toJSONString(new JsonTest());
        System.out.println(objectJSON);

        /*
         *   从JSON中解析对象
         * */

        JSONObject jsonTest = JSON.parseObject(objectJSON);
        JsonTest jsonTest1 = jsonTest.toJavaObject(JsonTest.class);
        System.out.println(jsonTest1);


    }

    public static class JsonTest {
        private int data = 0;

        public int getData() {
            return this.data;
        }

        public void setData(int data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "JsonTest{" +
                    "data=" + data +
                    '}';
        }
    }
}
