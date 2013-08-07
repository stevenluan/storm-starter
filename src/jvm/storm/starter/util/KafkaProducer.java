package storm.starter.util;

import java.util.Properties;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private final static Properties props = new Properties();
	static {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect",
				"zk-c1-15582.phx-os1.stratus.dev.ebay.com:2181");
	}
	private final static kafka.javaapi.producer.Producer<Integer, String> producer = new kafka.javaapi.producer.Producer<Integer, String>(
			new ProducerConfig(props));;
	private static KafkaProducer _instance = new KafkaProducer();
	private KafkaProducer() {

	}
	public static KafkaProducer getInstance(){
		return _instance;
	}
	public void send(String topic, String msg) {
		producer.send(new ProducerData<Integer, String>(topic, msg));

	}

}
