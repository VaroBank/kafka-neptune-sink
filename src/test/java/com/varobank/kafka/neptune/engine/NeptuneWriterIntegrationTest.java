/*
Copyright (c) 2022 Varo Bank, N.A. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package com.varobank.kafka.neptune.engine;

import com.rpuch.moretestcontainers.gremlinserver.GremlinServerContainer;
import com.varobank.common.gremlin.queries.GremlinQueriesObjectFactory;
import com.varobank.common.gremlin.queries.ReadQueries;
import com.varobank.common.gremlin.queries.WriteQueries;
import com.varobank.common.gremlin.utils.ConnectionCluster;
import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.NeptuneSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.utility.DockerImageName;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = { "spring.config.location=classpath:application-integrationtest.yaml" })
public class NeptuneWriterIntegrationTest {

    private boolean runGremlinDockerImage = true;
    private static final DockerImageName GREMLIN_IMAGE = DockerImageName.parse("tinkerpop/gremlin-server:3.4.10");
    private GremlinServerContainer gremlinServer = new GremlinServerContainer(GREMLIN_IMAGE);
    {
        if (runGremlinDockerImage) {
            gremlinServer.start();
        }
    }

    @Autowired
    private NeptuneBatchWriter neptuneBatchWriter;

    @Autowired
    private GremlinQueriesObjectFactory queriesObjectFactory;

    @Autowired
    private NeptuneSchema rawSchema;

    @MockBean
    private Consumer consumer;

    @SpyBean
    private ConnectionCluster cluster;

    public Cluster creteTestClsuter(String endpointUrl, String neptunePort, String neptuneSsl) {
        return Cluster.build()
                .addContactPoint(endpointUrl)
                .port(Integer.parseInt(neptunePort))
                .enableSsl(Boolean.parseBoolean(neptuneSsl))
                .serializer(Serializers.GRAPHBINARY_V1D0)
                .create();
    }

    public ConnectionConfig getConnectionConfig() throws Exception {
        ConnectionConfig conf = spy(ConnectionConfig.class);
        Cluster testCluster = creteTestClsuter("localhost", "8182", "false");
        ConnectionCluster cluster = spy(ConnectionCluster.class);
        Collection<String> endpoints = Arrays.asList("localhost");
        doReturn(endpoints).when(cluster).getEndpoints();
        doReturn(testCluster.connect()).when(cluster).connect();
        if (runGremlinDockerImage) {
            doReturn(gremlinServer.openConnection()).when(conf).createRemoteConnection();
        }
        when(cluster.isCreated()).thenReturn(true);
        doNothing().when(cluster).createCluster();
        doNothing().when(cluster).close();
        conf.setCluster(cluster);
        cluster.setNeptuneClusterId("localhost");
        cluster.setNeptunePort("8182");
        cluster.setNeptuneSsl("false");
        cluster.setNeptuneEndpointType("write");

        return conf;
    }

    private List<ConsumerRecord<String, String>> createRecords(String topic, String idField, String refKeyField, String childIdField, Integer idStartIdx, Integer refKeyIdx, Integer childIdStartIdx) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        String childId = childIdStartIdx.toString();
        String id = idStartIdx.toString();
        String op = "u";
        String refKey = refKeyField != null ? "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + refKeyField + "\"}," : "";
        String refValue = refKeyField != null ? "\"" + refKeyField + "\":" + refKeyIdx + "," : "";
        String key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"id\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Key\"},\"payload\":{\"" + idField + "\":" + id + "}}";
        String value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey +  "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey + "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"xmin\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"" + idField + "\":" + id + "," + refValue + "\"" + childIdField + "\":" + childId + ",\"status\":\"ACTIVE\",\"version\":0},\"source\":{\"version\":\"1.5.3.Final\",\"connector\":\"postgresql\",\"name\":\"auroraserver\",\"ts_ms\":1625191696107,\"snapshot\":\"false\",\"db\":\"customer\",\"sequence\":\"[\\\"2804081856\\\",\\\"2804081856\\\"]\",\"schema\":\"public\",\"table\":\"customer\",\"txId\":531413643,\"lsn\":2804132224,\"xmin\":null},\"op\":\"" + op + "\",\"ts_ms\":1625191696159,\"transaction\":null}}";
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(topic, 0, 0, 1623286626968L, TimestampType.CREATE_TIME, 0, 244, 3222, key, value);
        records.add(record);

        value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey +  "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey + "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"xmin\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"" + idField + "\":" + id + "," + refValue + "\"" + childIdField + "\":" + childId + ",\"status\":\"ACTIVE\",\"version\":0},\"source\":{\"version\":\"1.5.3.Final\",\"connector\":\"postgresql\",\"name\":\"auroraserver\",\"ts_ms\":1625191696108,\"snapshot\":\"false\",\"db\":\"customer\",\"sequence\":\"[\\\"2804081856\\\",\\\"2804081856\\\"]\",\"schema\":\"public\",\"table\":\"customer\",\"txId\":531413643,\"lsn\":2804132224,\"xmin\":null},\"op\":\"" + op + "\",\"ts_ms\":1625191696160,\"transaction\":null}}";
        record = new ConsumerRecord<String, String>(topic, 0, 0, 1623286626968L, TimestampType.CREATE_TIME, 0, 244, 3222, key, value);
        records.add(record);

        idStartIdx++;
        childIdStartIdx++;
        id = idStartIdx.toString();
        childId = childIdStartIdx.toString();
        refValue = refKeyField != null ? "\"" + refKeyField + "\":" + ++refKeyIdx + "," : "";
        op = "u";
        key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"id\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Key\"},\"payload\":{\"id\":" + id + "}}";
        value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey +  "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey + "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"xmin\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"" + idField + "\":" + id + "," + refValue + "\"" + childIdField + "\":" + childId + ",\"status\":\"ACTIVE\",\"version\":0},\"source\":{\"version\":\"1.5.3.Final\",\"connector\":\"postgresql\",\"name\":\"auroraserver\",\"ts_ms\":1625191696107,\"snapshot\":\"false\",\"db\":\"customer\",\"sequence\":\"[\\\"2804081856\\\",\\\"2804081856\\\"]\",\"schema\":\"public\",\"table\":\"customer\",\"txId\":531413643,\"lsn\":2804132224,\"xmin\":null},\"op\":\"" + op + "\",\"ts_ms\":1625191696159,\"transaction\":null}}";
        record = new ConsumerRecord<String, String>(topic, 0, 0, 1623286626968L, TimestampType.CREATE_TIME, 0, 244, 3222, key, value);
        records.add(record);

        op = "d";
        key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"id\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Key\"},\"payload\":{\"id\":" + id + "}}";
        value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey +  "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + idField + "\"}," + refKey + "{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.data.Uuid\",\"version\":1,\"field\":\"" + childIdField + "\"},{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"version\"}],\"optional\":true,\"name\":\"auroraserver.public.customer.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"xmin\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"auroraserver.public.customer.Envelope\"},\"payload\":{\"after\":null,\"before\":{\"" + idField + "\":" + id + "," + refValue + "\"" + childIdField + "\":" + childId + ",\"status\":\"ACTIVE\",\"version\":0},\"source\":{\"version\":\"1.5.3.Final\",\"connector\":\"postgresql\",\"name\":\"auroraserver\",\"ts_ms\":1625191696107,\"snapshot\":\"false\",\"db\":\"customer\",\"sequence\":\"[\\\"2804081856\\\",\\\"2804081856\\\"]\",\"schema\":\"public\",\"table\":\"customer\",\"txId\":531413643,\"lsn\":2804132224,\"xmin\":null},\"op\":\"" + op + "\",\"ts_ms\":1625191696159,\"transaction\":null}}";

        ConsumerRecord<String, String> delete = new ConsumerRecord<String, String>(topic, 0, 0, 1623286626968L, TimestampType.CREATE_TIME, 0, 244, 3222, key, value);
        records.add(delete);

        return records;
    }

    private void asserWrittenRecords(Map mapActual, ConsumerRecord<String, String> recordExpected, String op, String topic) {
        JSONObject json = new JSONObject(recordExpected.value());
        JSONObject payload = json.getJSONObject("payload");
        long ts_ms = payload.getLong("ts_ms");
        JSONObject after = payload.getJSONObject("after");
        after.put("ts_ms", ts_ms);
        after.put("op", op);
        after.put("topic", topic);

        after.keys().forEachRemaining(k -> {
            try {
                Object val = after.get((String) k);
                if (k.equals("topic")) {
                    assert mapActual.get(T.label).equals(val);
                } else if (k.equals("id")) {
                    assert mapActual.get(T.id).equals(((Integer)val).longValue());
                } else {
                    if (JSONObject.NULL == val && "{}".equals(mapActual.get(k))) {
                        assert true;
                    } else {
                        assert mapActual.get(k).equals(val);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        });

    }

    @Test
    public void writeToNeptuneTest() throws Exception {
        ConnectionConfig conf = getConnectionConfig();
        queriesObjectFactory.setConnectionConfig(conf);
        neptuneBatchWriter.setConnectionConfig(conf);

        ReadQueries nr = queriesObjectFactory.createReadQueries();
        WriteQueries nw = queriesObjectFactory.createWriteQueries();

        List<ConsumerRecord<String, String>> records = createRecords("cdc.customer.customer", "id", null, "identity_id", 1, null,101);
        List<ConsumerRecord<String, String>> loginRecords = createRecords("cdc.login.login", "identity_id", null, "test_id", 101, null, 201);
        List<ConsumerRecord<String, String>> ordersRecords = createRecords("cdc.orders.orders", "id", "customer_id", "service_id", 11, 1, 201);

        records.addAll(loginRecords);
        records.addAll(ordersRecords);

        nw.clear();
        neptuneBatchWriter.writeToNeptune(records);

        Map map = nr.getById("cdc.customer.customer", "2");
        asserWrittenRecords(map, records.get(2), "d", "cdc.customer.customer");

        Map customerMap = nr.getCustomerDataFromAllVertices("1");
        assert customerMap.containsKey("cdc.customer.customer");
        assert customerMap.containsKey("cdc.login.login");
        assert customerMap.containsKey("cdc.orders.orders");
        assert customerMap.containsKey("cdc.service.service");

        conf.close();
    }
}
