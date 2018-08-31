import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.transport.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
                .setSource(message, XContentType.JSON).get();
        /*
        查询操作
         */
        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
        /*
        Bulk Api
        简单来讲的话就是批量进行操作
         */

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("twitter", "tweet", "12")
        .setSource(jsonBuilder()
                    .startObject()
                        .field("user", "stev")
                        .field("message", "Bulk API prepareIndex trying").endObject()));

        bulkRequestBuilder.add(client.prepareUpdate("twitter", "tweet", "1")
        .setDoc(jsonBuilder().startObject()
                .field("gender", "male")
                .endObject()));

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            System.out.println("bulk op failed");
        }

        /*
        Bulk Processor
         */
        BulkProcessor bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    public void beforeBulk(long l, BulkRequest bulkRequest) {
                        System.out.println(bulkRequest.numberOfActions());
                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        System.out.println(bulkResponse.hasFailures());
                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                        System.out.println(throwable.getMessage());
                    }
                }).setBulkActions(100)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .build();
        System.out.println("twitter,tweet,id=1:" + response.toString());

        client.close();
    }
}
