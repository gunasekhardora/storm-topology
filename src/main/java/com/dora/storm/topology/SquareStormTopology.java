package com.dora.storm.topology;

import com.dora.storm.bolt.PrintBolt;
import com.dora.storm.bolt.SquareBolt;
import com.dora.storm.commons.Constants;
import com.dora.storm.spout.NumberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class SquareStormTopology {

    public static void main(String args[]) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(Constants.NUMBER_SPOUT, new NumberSpout());
        topologyBuilder.setBolt(Constants.SQUARE_BOLT, new SquareBolt()).shuffleGrouping(Constants.NUMBER_SPOUT);
        topologyBuilder.setBolt(Constants.PRINT_BOLT, new PrintBolt()).shuffleGrouping(Constants.SQUARE_BOLT);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constants.SQUARE_STORM_TOPOLOGY, config, topologyBuilder.createTopology());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {

        }

        cluster.killTopology(Constants.SQUARE_STORM_TOPOLOGY);
        cluster.shutdown();
    }
}
