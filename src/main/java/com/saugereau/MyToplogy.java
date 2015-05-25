package com.saugereau;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.saugereau.bolt.CassandraBolt;
import com.saugereau.spout.TwitterSpout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Jabberwock on 16/11/2014.
 */
public class MyToplogy {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        try {
            Properties properties = new Properties();
            properties.load(MyToplogy.class.getClassLoader().getResourceAsStream("mystorm.properties"));

            TwitterSpout twitterSpout = new TwitterSpout(
                    properties.getProperty("consumerKey"),
                    properties.getProperty("consumerSecret"),
                    properties.getProperty("accessToken"),
                    properties.getProperty("accessTokenSecret"),
                    new String[]{}
            );
            builder.setSpout("spout", twitterSpout, 1);
            builder.setBolt("split", new CassandraBolt(), 2).shuffleGrouping("spout");

            Config conf = new Config();
            conf.setDebug(true);

            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("cassandra-register", conf, builder.createTopology());

            Thread.sleep(200000);

            cluster.shutdown();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
