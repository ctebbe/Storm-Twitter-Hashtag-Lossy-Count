package bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import util.Util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ctebbe
 * receives incoming hashtags and writes them to a log file
 */
public class LogBolt extends BaseRichBolt {

    private PrintWriter writer;
    private OutputCollector collector;

    private final String filename;

    public LogBolt(String fname) {
        filename = fname;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            writer = new PrintWriter(filename,"UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String hashtags = tuple.getStringByField("hashtags").toLowerCase();
        writer.println(Util.getTimeStamp() +":"+ hashtags);
        writer.flush();
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}
