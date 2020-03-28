package com.atguigu.ct.consumer.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.atguigu.ct.common.bean.BaseDao;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import com.atguigu.ct.consumer.bean.Calllog;

/**
 * Hbase的数据访问对象
 */
public class HbaseDao extends BaseDao {

    

	/**
     * 初始化
     */
    public void init() throws  Exception{
        start();
        createNamespceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),"com.atguigu.ct.sonsumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());

        end();
    }

    /**
     * 插入对象
     * @param log
     * @throws Exception
     */
    public void insertData(Calllog log) throws Exception{
    	log.setRowKey(genRegionNum(log.getCall1(),log.getCalltime()) + "_" + log.getCall1() + "_" 
                               + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration());
    	
    	putData(log);
    }

	/**
     * 插入数据
     * @param value
     */
    public void insertData(String value)throws Exception{
    	//获取通话数据
    	String[] spilt = value.split("\t");
    	String call1 = spilt[0];
    	String call2 = spilt[1];
    	String calltime = spilt[2];
    	String duration = spilt[3];
    	//创建数据对象
    	String rowKey = genRegionNum(call1,calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
    	Put put = new Put(Bytes.toBytes(rowKey));
    	byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
    	put.addColumn(family, Bytes.toBytes("call1"),Bytes.toBytes(call1));
    	put.addColumn(family, Bytes.toBytes("call2"),Bytes.toBytes(call2));
    	put.addColumn(family, Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
    	put.addColumn(family, Bytes.toBytes("duration"),Bytes.toBytes(duration));
    	put.addColumn(family, Bytes.toBytes("flg"),Bytes.toBytes("1"));
    	
    	
    	
        //String calleeRowkey = genRegionNum(call2, calltime) + "_" + call2 + "_" + 
                       //calltime + "_" + call1 + "_" + duration + "_0";

        // 被叫用户
//        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
//        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
//        calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));

        // 3. 保存数据
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        //puts.add(calleePut);
        putData(Names.TABLE.getValue(), puts);
    }

	
}
