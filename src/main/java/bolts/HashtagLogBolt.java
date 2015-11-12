package bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

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
public class HashtagLogBolt extends BaseRichBolt {

    private PrintWriter writer;
    private OutputCollector collector;

    private final String filename;
    private Map<String, List<String>> tagMap = new ConcurrentHashMap<String, List<String>>();

    public HashtagLogBolt(String fname) {
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
        String hashtag = tuple.getStringByField("hashtag").toLowerCase();
        String timestamp = new SimpleDateFormat("HH:mm:ss:SS").format(new Date());
        System.out.println("***NOT TICK***");
        try {
            if(isTickTuple(tuple)) { // emit counts
                System.out.println("***TICK***");
            } else {
                recordHashTag(timestamp, hashtag);
                collector.ack(tuple);
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple != null
            && tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    private void recordHashTag(String timestamp, String hashtag) {
        if(tagMap.containsKey(hashtag)) {
            tagMap.get(hashtag).add(timestamp);
        } else {
            tagMap.put(hashtag, new ArrayList<String>());
            tagMap.get(hashtag).add(timestamp);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void cleanup() {
        for(Map.Entry<String,List<String>> entry : tagMap.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey() + "\t");
            for(String ts : entry.getValue())
                sb.append(ts+"; ");
            sb.append("\n");
            writer.print(sb.toString());
        }
        writer.flush();
        writer.close();
        super.cleanup();
    }
}
