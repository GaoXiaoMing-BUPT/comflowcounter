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
     *   ���뻺��,�����ѯ���ݿⷽ��
     * */
    static {
        areaHshMap.put("1351", 0);
        areaHshMap.put("1352", 1);
    }

    /*
     *   ��key���õ��ֻ��ţ���ѯ�ֻ��Ź����أ���ͬʡ�ݷ��ز�ͬ�����
     *   �߲�����ѯ�����������ж���һ����̬����飬�����Ҫ����ʡ�ݵĵ绰�����
     * */
    @Override
    public int getPartition(KEY key, VALUE value, int i) {
        int areaCoder = areaHshMap.get(key.toString().substring(0, 4))
                == null ? 2 : areaHshMap.get(key.toString().substring(0, 4));
        return areaCoder;
    }
}
