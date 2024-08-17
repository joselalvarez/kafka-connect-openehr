package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseEventLogService;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseEventOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.EhrStatusEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.RecordPartitionFactory;
import com.github.joselalvarez.openehr.connect.source.task.OpenEHREventLogService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import net.almson.object.ReferenceCountedObject;
import org.apache.commons.dbutils.QueryRunner;
import org.ehrbase.openehr.sdk.serialisation.jsonencoding.CanonicalJson;

import javax.sql.DataSource;

@Slf4j
class EHRBaseSourceConnectorContext extends ReferenceCountedObject implements OpenEHRSourceConnectorContext {

    private boolean closed;

    private OpenEHRSourceConnectorConfig connectorConfig;
    private HikariDataSource hikariDataSource;
    private EHRBaseRepository ehrBaseRepository;
    private EHRBaseEventLogService eventLogService;
    private ObjectMapper canonicalObjectMapper;
    private CompositionEventRecordMapper compositionEventRecordMapper;
    private EhrStatusEventRecordMapper ehrStatusEventRecordMapper;
    private EHRBaseEventOffsetFactory eventLogOffsetFactory;

    public EHRBaseSourceConnectorContext(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        // DataSource
        hikariDataSource = new HikariDataSource(new HikariConfig(connectorConfig.getJdbcProperties()));
        log.info("Connector[name={}]: EHRBase datasource created", connectorConfig.getConnectorName());
        // Beans
        eventLogOffsetFactory = new EHRBaseEventOffsetFactory(connectorConfig);
        ehrBaseRepository = new EHRBaseRepository(connectorConfig, new QueryRunner(hikariDataSource), eventLogOffsetFactory);
        eventLogService = new EHRBaseEventLogService(ehrBaseRepository);
        canonicalObjectMapper = CanonicalJson.MARSHAL_OM;
        canonicalObjectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        compositionEventRecordMapper = new CompositionEventRecordMapper(canonicalObjectMapper);
        ehrStatusEventRecordMapper = new EhrStatusEventRecordMapper(canonicalObjectMapper);

    }

    @Override
    public DataSource getEHRBaseDataSource() {
        return hikariDataSource;
    }

    @Override
    public OpenEHREventLogService getOpenEHREventLogService() {
        return eventLogService;
    }

    @Override
    public ObjectMapper getCanonicalObjectMapper() {
        return canonicalObjectMapper;
    }

    @Override
    public CompositionEventRecordMapper getCompositionEventRecordMapper() {
        return compositionEventRecordMapper;
    }

    @Override
    public EhrStatusEventRecordMapper getEhrStatusEventRecordMapper() {
        return ehrStatusEventRecordMapper;
    }

    @Override
    public RecordPartitionFactory getRecordPartitionFactory() {
        return eventLogOffsetFactory;
    }

    @Override
    protected void destroy() {
        closed = true;
        hikariDataSource.close();
        log.info("Connector[name={}]: EHRBase datasource closed", connectorConfig.getConnectorName());
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
