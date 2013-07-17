package storm.starter.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CollectionScheme implements Scheme {
	
	public List<Object> deserialize(byte[] bytes) {
		ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
        try {
        	Map<String,Object> collectionData = mapper.readValue(new String(bytes, "UTF-8"), Map.class);
            return new Values(collectionData.get("collId"), collectionData.get("action"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (JsonParseException e) {
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

    public Fields getOutputFields() {
        return new Fields("collId", "action");
    }

}
