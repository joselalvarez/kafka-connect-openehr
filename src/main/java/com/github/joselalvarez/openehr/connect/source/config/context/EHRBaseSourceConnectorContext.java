package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.EhrStatusEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.service.ehrbase.EHRBaseEventLogService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import net.almson.object.ReferenceCountedObject;
import org.apache.commons.dbutils.QueryRunner;
import com.github.joselalvarez.openehr.connect.source.repository.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHREventLogService;
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

    public EHRBaseSourceConnectorContext(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        // DataSource
        hikariDataSource = new HikariDataSource(new HikariConfig(connectorConfig.getJdbcProperties()));
        log.info("Connector[name={}]: EHRBase datasource created", connectorConfig.getConnectorName());
        // Beans
        ehrBaseRepository = new EHRBaseRepository(new QueryRunner(hikariDataSource));
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
