package com.fms.etelastic.elasticexample.resource;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RestController
@RequestMapping("/rest/event")
public class UsersResource {
//    Note that you have to set the cluster name if you use one different than "elasticsearch":
//Settings settings = Settings.builder()
//        .put("cluster.name", "myClusterName").build();

    TransportClient client;

    public UsersResource() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

    }

    @GetMapping("/insert/{id}")
    public String insert(@PathVariable final String id) throws UnknownHostException {
//        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
//                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
//        System.out.println(client.connectedNodes());
        try {
            IndexResponse response = client.prepareIndex("event", "id", id)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "untitled")
                            .field("id", "67")
                            .endObject()).get();
            return response.getResult().toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @GetMapping("/view")
    public Map<String, Object> view() {
        GetResponse response = client.prepareGet("events", "id", "1").get();
        return response.getSource();
    }

    @GetMapping("/update/{id}")
    public String update(@PathVariable final String id) throws ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        try {
            request.index("event")
                    .type("id")
                    .id(id)
                    .doc(jsonBuilder()
                            .startObject()
                            .field("name", "updatetitle")
                            .endObject());
        } catch (IOException e) {
            e.printStackTrace();
        }
        UpdateResponse response = client.update(request).get();
        return response.status().toString();
    }

    @GetMapping("/delete/{id}")
    public String delete(@PathVariable final String id) {
        DeleteResponse response = client.prepareDelete("event", "id", id).get();
        return response.status().toString();
    }

    @GetMapping("/bulkinsert")
    public String bulkInsert() {

        try {
            CreateIndexRequest request = new CreateIndexRequest("events");
//            request.
//            CreateIndexResponse createIndexResponse = client.prepareIndex(request);//.indices().create(request, RequestOptions.DEFAULT);

//            request.settings(Settings.builder()
//                    .put("index.number_of_shards", 3)
//                    .put("index.number_of_replicas", 2)
//            );

            BulkRequestBuilder bulkRequest = client.prepareBulk();
            bulkRequest.add(client.prepareIndex("events", "_doc", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    )
            );

            bulkRequest.add(client.prepareIndex("events", "_doc", "2")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "another post")
                            .endObject()
                    )
            );
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
            }
            return bulkResponse.status().toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Failed";
    }

    @GetMapping("/viewall")
    public void viewall() {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("events", "_doc", "1")
                .add("events", "_doc", "2", "3", "4")
                //.add("another", "_doc", "foo")
                .get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);

            }
        }
        long totaldoc = client.admin().indices().prepareStats("events").get().getTotal().getDocs().getCount();
        System.out.println(totaldoc);

    }

    @GetMapping("/search")
    public List<String> getAllDocs(){
        int scrollSize = 1000;
        List<String> esData = new ArrayList<String>();
//        SearchResponse response = client.prepareSearch("events")
//                .setTypes("_doc")
//                .setQuery(QueryBuilders.matchAllQuery())
//                .setSize(10)
////                    .setFrom(i * scrollSize)
//                .execute()
//                .actionGet();

//        SearchResponse response = client.prepareSearch().get();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(
                QueryBuilders.matchQuery("message", "another post"));
        SearchResponse response = client.prepareSearch("events")//, "index2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
//                .setQuery(QueryBuilders.termQuery("message", "another post"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        for(SearchHit hit : response.getHits()){
            esData.add(hit.getSourceAsString());
        }
        return esData;
    }
}
