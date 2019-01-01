package com.x.flink;

import com.x.flink.util.MD5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.x.flink.config.UserConfig.*;
import static com.x.flink.util.Constant.*;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Collections.singletonMap;

public class EsSink extends RichSinkFunction<String[]> {

    private static final long serialVersionUID = -4773550304884486333L;
    private static final String CODE = "ctx._source." + STATISTICAL_FIELD + " += params." + STATISTICAL_FIELD;
    private static final String LANG = "painless";
    private static final String SCHEME = "http";
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(1);
    private static final int BATCH_SIZE = 100;
    private RestHighLevelClient client;
    private BulkRequest bulkRequest;

    @Override
    public void open(Configuration parameters) {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(ES_HOSTNAME, ES_PORT, SCHEME)));
        bulkRequest = new BulkRequest();
    }

    @Override
    public void close() throws Exception {
        if (client != null) client.close();
    }

    @Override
    public void invoke(String[] results, Context context) {
        int groupIndex = parseInt(results[results.length - 1]);
        String[] groupFields = groupConfigs[groupIndex].split(SEPARATOR);
        String esIndexName = esIndexNames.get(groupIndex);
        String esId = MD5.md(String.join(CONNECTOR, Arrays.copyOfRange(results, 0, groupFields.length)));
        UpdateRequest updateRequest = new UpdateRequest(esIndexName, ES_TYPE, esId);
        String sum = results[groupFields.length];
        Map<String, Object> parameters = singletonMap(STATISTICAL_FIELD, parseDouble(sum));
        updateRequest.script(new Script(ScriptType.INLINE, LANG, CODE, parameters));
        Map<String, Object> upsertMap = new HashMap<>(parameters);
        upsertMap.put(groupFields[TIME_INDEX], new Date(parseLong(results[TIME_INDEX])));
        IntStream.range(1, groupFields.length).forEach(j -> upsertMap.put(groupFields[j], results[j]));
        updateRequest.upsert(upsertMap);
        updateRequest.timeout(TIMEOUT);
        batchUpdateToEs(updateRequest);
    }

    private void batchUpdateToEs(UpdateRequest updateRequest) {
        try {
            bulkRequest.add(updateRequest);
            if (bulkRequest.numberOfActions() >= BATCH_SIZE) {
                long start = System.currentTimeMillis();
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulkRequest.requests().clear();
                for (BulkItemResponse response : bulkResponse) {
                    indexToEs((response.getResponse()), start);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void indexToEs(UpdateResponse updateResponse, long start) {
        IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_TYPE);
        Map<String, Object> indexMap = new HashMap<>();
        indexMap.put("second", new Date());
        indexMap.put("result", updateResponse.getResult());
        indexMap.put("cost", System.currentTimeMillis() - start);
        indexRequest.source(indexMap);
        try {
            bulkRequest.add(indexRequest);
            if (bulkRequest.numberOfActions() >= BATCH_SIZE) {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulkRequest.requests().clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
