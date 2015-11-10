import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolts.FileWriterBolt;
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
        builder.setBolt("file-writer", new FileWriterBolt("tweets.txt"), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter", conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.shutdown();
    }
}
