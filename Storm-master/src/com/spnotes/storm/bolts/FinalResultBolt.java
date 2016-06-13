package com.spnotes.storm.bolts;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class FinalResultBolt implements IRichBolt {

    Map<String, Integer> counters;
    Set<String> setCounter;
    Set<String> dupCounter;
    Set<String> dupCounterFinal;
    Map<String, Long> longestActivity;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.longestActivity = new HashMap<String, Long>();
        this.setCounter = new HashSet<String>();
        this.dupCounter = new HashSet<String>();
        this.dupCounterFinal = new HashSet<String>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        //times, a particular product has been launched
        if (!str.isEmpty()) {
            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            //first-time launches
            setCounter.add(str);
        }
        //Duplicate events
        str = input.getString(1);
        if (!str.isEmpty()) {
            if (dupCounter.contains(str))
                    dupCounterFinal.add(str);

            dupCounter.add(str);
        }
        //longest 'activity time'
        str = input.getString(2);
        if (!str.isEmpty()) {
            String abc[] = str.split(":");
            longestActivity.put(abc[0], Long.parseLong(abc[1]));
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        System.out.println("\nResults\n");
        System.out.println("Particular product has been launched:\n");
        System.out.println("Product ID : Number of times\n");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("\nNumber of first-time launches : " + setCounter.size());
        System.out.println("\nNumber of duplicate events : " + dupCounterFinal.size());
        System.out.println("\nLongest activity Device ID : " +
                longestActivity.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
