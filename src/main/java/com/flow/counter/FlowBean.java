package com.flow.counter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable, WritableComparable<FlowBean> {
    private String phoneNumber;
    private long upFlow;
    private long downFlow;
    private long totalFlow;

    //若写入参数，无法解析，必须显示写入空参构造方法方便反射机制调用，带参为自己使用

    public FlowBean() {
    }

    //自用的对象初始化
    public FlowBean(String phoneNumber, long upFlow, long downFlow) {
        this.phoneNumber = phoneNumber;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = this.downFlow + this.upFlow;
    }

    public String getPhoneNumber() {
        return this.phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public long getUpFlow() {
        return this.upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return this.downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        return this.totalFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override//对象序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phoneNumber);
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.totalFlow);
    }

    @Override//反序列化读取
    public void readFields(DataInput in) throws IOException {
        this.phoneNumber = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    @Override
    public String toString() {
        return
                phoneNumber +
                        " " + upFlow +
                        " " + downFlow +
                        " " + totalFlow
                ;
    }

    @Override
    public int compareTo(FlowBean o) {
        return totalFlow > o.totalFlow ? -1 : (totalFlow == o.totalFlow ? 0 : 1);
    }
}
