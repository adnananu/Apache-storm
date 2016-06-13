package com.spnotes.storm;

import com.spnotes.storm.bolts.FinalResultBolt;
import com.spnotes.storm.bolts.DataProcessBolt;
import com.spnotes.storm.spouts.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MainStorm {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("inputFile", "C:\\Users\\Adnan\\Desktop\\test\\obfuscated_data");
        config.setDebug(true);
        config.setNumWorkers(8);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5);
        config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16);
        config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        //building topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LineReaderSpout.class.getSimpleName(), new LineReaderSpout(), 1);
        builder.setBolt(DataProcessBolt.class.getSimpleName(), new DataProcessBolt(), 8).
                setNumTasks(6).shuffleGrouping(LineReaderSpout.class.getSimpleName());
        builder.setBolt(FinalResultBolt.class.getSimpleName(), new FinalResultBolt(), 1).
                shuffleGrouping(DataProcessBolt.class.getSimpleName());
        //submitting topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(MainStorm.class.getSimpleName(), config, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology(MainStorm.class.getSimpleName());
        cluster.shutdown();
    }

}
