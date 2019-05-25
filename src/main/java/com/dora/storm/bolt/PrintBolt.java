package com.dora.storm.bolt;

import com.dora.storm.commons.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class PrintBolt extends BaseRichBolt {
    private HashMap<Integer, Integer> numSquare = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        numSquare = new HashMap<Integer, Integer>();
    }

    public void execute(Tuple tuple) {
        Integer number = tuple.getIntegerByField(Constants.NUMBER);
        Integer result = tuple.getIntegerByField(Constants.NUMBER_SQUARE);
        this.numSquare.put(number, result);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }

    public void cleanup() {
        System.out.println("----------RESULTS----------");
        List<Integer> keys = new ArrayList<Integer>();
        keys.addAll(this.numSquare.keySet());
        Collections.sort(keys);
        for (Integer key : keys) {
            System.out.println(key + " : " + this.numSquare.get(key));
        }
        System.out.println("---------------------------");
    }
}
