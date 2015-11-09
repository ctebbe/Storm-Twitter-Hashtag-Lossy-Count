package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by ctebbe
 */
public class FileWriterBolt extends BaseRichBolt {

    private PrintWriter writer;
    private OutputCollector collector;

    private final String filename;
    private int count;

    public FileWriterBolt(String fname) {
        filename = fname;
        count = 0;
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
        writer.println((count++) + ":" + tuple);
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
