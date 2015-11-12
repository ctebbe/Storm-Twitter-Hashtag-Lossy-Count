package bolts;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import util.Util;

import java.util.Map;

/**
 * Created by ctebbe
 */
public class LossyCountingAggregatorBolt extends BaseBasicBolt {

    private int n;
    private double e;
    OutputCollector collector;

    public LossyCountingAggregatorBolt(double e) {
        this.e = e;
        this.n = 0;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(Util.isTickTuple(tuple)) {
            emitFreqs(basicOutputCollector);
        } else {

        }
    }

    private void emitFreqs(BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "frequency", "delta"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}
