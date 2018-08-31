import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.transport.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class esTest {
    public static void main(String[] args) throws IOException {

        byte[] addr = {127, 0, 0, 1};
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("user", "martinTest").put("message", "Test On ElesticSearch 6.4.0");
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(0, "test41").put(1, "test42");
        jsonObject.put("testArray", jsonArray);
        String message = jsonObject.toString();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("user", "martin");
        map.put("message", "11");
        map.put("arrays", jsonArray);
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByAddress(addr), 9300));


        /*
        插入，更新操作
         */
        IndexResponse indexResponse = client.prepareIndex("twitter", "tweet", "15")
              /*  .setSource(jsonBuilder()
                                .startObject()
                                .field("user", "kimchy")
                                .field("postDate", new Date())
                                .field("message", "trying out Elasticsearch")
                                .endObject()
                )
                .get()  ;*/.setSource(message, XContentType.JSON).get();
        /*
        查询操作
         */
        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();

        System.out.println("twitter,tweet,id=1:" + response.toString());

                        client.close();
    }
}
