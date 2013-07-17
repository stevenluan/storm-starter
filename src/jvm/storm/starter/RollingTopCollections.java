package storm.starter;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountOfCollectionBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.util.CollectionScheme;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the
 * topology has seen in terms of cardinality. The top N computation is done in a
 * completely scalable way, and a similar approach could be used to compute
 * things like trending topics or trending images on Twitter.
 */
public class RollingTopCollections {

	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 10;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public RollingTopCollections() throws InterruptedException {
		builder = new TopologyBuilder();
		topologyName = "slidingWindowCounts";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "wordGenerator";
		String counterId = "counter";
		String intermediateRankerId = "intermediateRanker";
		String totalRankerId = "finalRanker";
		SpoutConfig spoutConfig = new SpoutConfig(new KafkaConfig.ZkHosts(
				"zkserver1-13722.phx-os1.stratus.dev.ebay.com", "/brokers"),
				"collection", "/kafkastorm", "discovery");
		spoutConfig.scheme = new CollectionScheme();
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		builder.setSpout(spoutId, kafkaSpout, 5);
		builder.setBolt(counterId, new RollingCountOfCollectionBolt(9, 3), 4)
				.fieldsGrouping(spoutId, new Fields("collId"));
		builder.setBolt(intermediateRankerId,
				new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(
				counterId, new Fields("obj"));
		builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N))
				.globalGrouping(intermediateRankerId);
	}

	public void run(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		if (args != null && args.length > 0) {
			topologyConfig.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], topologyConfig,
					builder.createTopology());
		} else {
			StormRunner.runTopologyLocally(builder.createTopology(),
					topologyName, topologyConfig, runtimeInSeconds);
		}
	}

	public static void main(String[] args) throws Exception {
		new RollingTopCollections().run(args);
	}
}