package storm.starter;

import java.util.Map;

import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableList;

/**
 * This is a basic example of a Storm topology.
 */
public class KafkaExclamationTopology {
    
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
        	System.out.println("tuplevalue ========"+tuple.getString(0));
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


    }
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig spoutConfig = new SpoutConfig(StaticHosts.fromHostString(ImmutableList.of("kafka1-13745.phx-os1.stratus.dev.ebay.com", "kafka2-13746.phx-os1.stratus.dev.ebay.com"), 1), "test", "/kafkastorm", "discovery"
		  );
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("word", kafkaSpout, 10);        
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
                
        Config conf = new Config();
       if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        	conf.put(conf.STORM_ZOOKEEPER_SERVERS, ImmutableList.of("zkserver1-13722.phx-os1.stratus.dev.ebay.com"));
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            //Utils.sleep(10000);
            //cluster.killTopology("test");
            //cluster.shutdown();    
        }
    }
}
