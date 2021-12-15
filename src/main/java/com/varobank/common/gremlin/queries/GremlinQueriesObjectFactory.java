package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GremlinQueriesObjectFactory {

    @Autowired
    private Schema schema;

    @Autowired
    private ConnectionConfig connectionConfig;

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public BatchWriteQueries createBatchWriteQueries() {
        return new BatchWriteQueries(connectionConfig, schema);
    }

    public ReadQueries createReadQueries() {
        return new ReadQueries(connectionConfig, schema);
    }

    public WriteQueries createWriteQueries() {
        return new WriteQueries(connectionConfig, schema);
    }
}
