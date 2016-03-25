package com.nano.spouts;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/22.
 */
public class MessageSpout implements IBatchSpout {

    private int index;
    private List<Map<String, Object>> sales;

    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
        sales = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> sale = new HashMap<>();
            if( i % 3 == 0) {
                sale.put("barcode", "abc000");
                sale.put("cost", 2.00f);
                sale.put("sale", 1.00f);
            }else if (i % 3 == 1) {
                sale.put("barcode", "abc001");
                sale.put("cost", 3.00f);
                sale.put("sale", 2.00f);
            }else {
                sale.put("barcode", "abc001");
                sale.put("cost", 5.00f);
                sale.put("sale", 2.00f);
            }
            sale.put("owner", "王五");
            sale.put("department", "产三");
            sale.put("count", i);
            sales.add(sale);
        }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        if (index++ < 1) {
            for (Map<String, Object> sale : sales) {
                Values values = new Values(sale.get("barcode"), sale.get("owner"), sale.get("department"), sale.get("cost"), sale.get("sale"), sale.get("count"));
                collector.emit(values);
            }
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("barcode", "owner", "department", "cost", "sale", "count");
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }
}
