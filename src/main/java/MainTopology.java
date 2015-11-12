import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolts.HashtagExtractBolt;
import bolts.LogBolt;
import spout.TwitterSpout;
import twitter4j.FilterQuery;

/**
 * Created by ct.
 */
public class MainTopology {

    public static void main(String[] args) {
        FilterQuery filter = null; //new FilterQuery();
        //filter.track(new String[]{"#"});

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwitterSpout(filter));

        builder.setBolt("hashtags", new HashtagExtractBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("log", new LogBolt("hashtags.log"), 1).shuffleGrouping("hashtags");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter", conf, builder.createTopology());

        Utils.sleep(30000);
        cluster.shutdown();
    }
}
