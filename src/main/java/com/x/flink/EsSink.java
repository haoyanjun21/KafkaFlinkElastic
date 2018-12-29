package com.x.flink;

import com.x.flink.util.MD5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
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

import static com.x.flink.config.UserConfig.*;
import static com.x.flink.util.Constant.*;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Collections.singletonMap;

public class EsSink extends RichSinkFunction<String[]> {

    private static final long serialVersionUID = 887304812850207936L;
    private static final String CODE = "ctx._source." + STATISTICAL_FIELD + " += params." + STATISTICAL_FIELD;
    private static final String LANG = "painless";
    private static final String SCHEME = "http";
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(1);
    private RestHighLevelClient client;

    @Override
    public void open(Configuration parameters) {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(ES_HOSTNAME, ES_PORT, SCHEME)));
    }

    @Override
    public void close() throws Exception {
        if (client != null) client.close();
    }

    @Override
    public void invoke(String[] values, Context context) {
        int groupIndex = parseInt(values[values.length - 1]);
        String[] groupFields = groupConfigs[groupIndex].split(SEPARATOR);
        String esIndexName = esIndexNames.get(groupIndex);
        String id = String.join(CONNECTOR, Arrays.copyOfRange(values, 0, groupFields.length));
        String esId = MD5.md(id);
        UpdateRequest request = new UpdateRequest(esIndexName, ES_TYPE, esId);
        String sum = values[groupFields.length];
        Map<String, Object> parameters = singletonMap(STATISTICAL_FIELD, parseDouble(sum));
        request.script(new Script(ScriptType.INLINE, LANG, CODE, parameters));
        Map<String, Object> map = new HashMap<>(parameters);
        map.put(groupFields[TIME_INDEX], new Date(parseLong(values[TIME_INDEX])));
        for (int j = 1; j < groupFields.length; j++) {
            map.put(groupFields[j], values[j]);
        }
        request.upsert(map);
        request.timeout(TIMEOUT);
        try {
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("EsSink created");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("EsSink updated");
            } else {
                System.out.println("EsSink other");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
