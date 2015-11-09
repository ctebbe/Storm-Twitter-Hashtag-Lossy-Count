import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.FileWriterBolt;
import spout.TwitterSpout;

/**
 * Created by ct.
 */
public class MainTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwitterSpout());
        builder.setBolt("file-writer", new FileWriterBolt("tweets.txt"), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter", conf, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
           // e.printStackTrace();
        }

        cluster.shutdown();
    }
}
