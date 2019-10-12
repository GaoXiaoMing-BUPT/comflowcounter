/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 11:50
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
    private static HashMap<String, Integer> areaHshMap = new HashMap<String, Integer>();

    /*
     *   存入缓存,比如查询数据库方法
     * */
    static {
        areaHshMap.put("1351", 0);
        areaHshMap.put("1352", 1);
    }

    /*
     *   从key中拿到手机号，查询手机号归属地，不同省份返回不同的组号
     *   高并发查询，可以在类中定义一个静态代码块，存放需要搜索省份的电话号码表
     * */
    @Override
    public int getPartition(KEY key, VALUE value, int i) {
        int areaCoder = areaHshMap.get(key.toString().substring(0, 4))
                == null ? 2 : areaHshMap.get(key.toString().substring(0, 4));
        return areaCoder;
    }
}
