package storm.starter.bolt;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import storm.starter.tools.Rankings;
import storm.starter.util.KafkaProducer;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class CollectionKafkaOutBolt extends BaseBasicBolt {
	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
		ObjectMapper mapper = new ObjectMapper(); 
	    
    	Rankings rankings = (Rankings) tuple.getValue(0);
    	try {
			String rankingsAsString = mapper.writeValueAsString(rankings);
			KafkaProducer.getInstance().send("TopCollections", rankingsAsString);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}
