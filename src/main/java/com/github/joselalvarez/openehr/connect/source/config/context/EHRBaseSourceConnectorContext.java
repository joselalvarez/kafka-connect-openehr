package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseChangeLogService;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseRecordOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeRecordMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeRecordMapper;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;
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
    private EHRBaseChangeLogService changeLogService;
    private ObjectMapper canonicalObjectMapper;
    private CompositionChangeRecordMapper compositionChangeRecordMapper;
    private EhrStatusChangeRecordMapper ehrStatusChangeRecordMapper;
    private EHRBaseRecordOffsetFactory recordOffsetFactory;

    public EHRBaseSourceConnectorContext(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        // DataSource
        hikariDataSource = new HikariDataSource(new HikariConfig(connectorConfig.getJdbcProperties()));
        log.info("Connector[name={}]: EHRBase datasource created", connectorConfig.getConnectorName());
        // Beans
        recordOffsetFactory = new EHRBaseRecordOffsetFactory(connectorConfig);
        ehrBaseRepository = new EHRBaseRepository(connectorConfig, new QueryRunner(hikariDataSource));
        changeLogService = new EHRBaseChangeLogService(ehrBaseRepository);
        canonicalObjectMapper = CanonicalJson.MARSHAL_OM;
        canonicalObjectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        compositionChangeRecordMapper = new CompositionChangeRecordMapper(canonicalObjectMapper, connectorConfig);
        ehrStatusChangeRecordMapper = new EhrStatusChangeRecordMapper(canonicalObjectMapper, connectorConfig);

    }

    @Override
    public DataSource getEHRBaseDataSource() {
        return hikariDataSource;
    }

    @Override
    public OpenEHRChangeLogService getOpenEHREventLogService() {
        return changeLogService;
    }

    @Override
    public ObjectMapper getCanonicalObjectMapper() {
        return canonicalObjectMapper;
    }

    @Override
    public CompositionChangeRecordMapper getCompositionChangeRecordMapper() {
        return compositionChangeRecordMapper;
    }

    @Override
    public EhrStatusChangeRecordMapper getEhrStatusChangeRecordMapper() {
        return ehrStatusChangeRecordMapper;
    }

    @Override
    public RecordOffsetFactory getRecordOffsetFactory() {
        return recordOffsetFactory;
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
