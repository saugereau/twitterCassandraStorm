package com.saugereau.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by Jabberwock on 18/11/2014.
 */
public class CassandraBolt extends BaseRichBolt {

    static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private Session _session;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        Cluster cluster = Cluster.builder().addContactPoint("52.16.218.87").build();
        _session = cluster.connect("tweetExp");

    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValue(0);

        PreparedStatement statement = _session.prepare(
                "INSERT INTO tweets " +
                        "(id, createdAt, textT, source, lang, userName, userScreenName, userLocation, userDescription, userUrl, userTimeZone, userLang) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.bind(
                tweet.getId(),
                tweet.getCreatedAt(),
                tweet.getText(),
                tweet.getSource(),
                tweet.getLang(),
                tweet.getUser().getName(),
                tweet.getUser().getScreenName(),
                tweet.getUser().getLocation(),
                tweet.getUser().getDescription(),
                tweet.getUser().getURL(),
                tweet.getUser().getTimeZone(),
                tweet.getUser().getLang());

        _session.execute(boundStatement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanUp() {
        _session.close();
    }

}
