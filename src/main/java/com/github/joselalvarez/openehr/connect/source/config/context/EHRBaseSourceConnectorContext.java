package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseChangeLogService;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBasePartitionOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.ehrbase.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeMapper;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;
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
    private CompositionChangeMapper compositionChangeRecordMapper;
    private EhrStatusChangeMapper ehrStatusChangeRecordMapper;
    private EHRBasePartitionOffsetFactory partitionOffsetFactory;

    public EHRBaseSourceConnectorContext(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        // DataSource
        hikariDataSource = new HikariDataSource(new HikariConfig(connectorConfig.getJdbcProperties()));
        log.info("Connector[name={}]: EHRBase datasource created", connectorConfig.getConnectorName());
        // Beans
        partitionOffsetFactory = new EHRBasePartitionOffsetFactory(connectorConfig);
        ehrBaseRepository = new EHRBaseRepository(connectorConfig, new QueryRunner(hikariDataSource));
        changeLogService = new EHRBaseChangeLogService(ehrBaseRepository);
        canonicalObjectMapper = CanonicalJson.MARSHAL_OM;
        canonicalObjectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        compositionChangeRecordMapper = new CompositionChangeMapper(canonicalObjectMapper, connectorConfig);
        ehrStatusChangeRecordMapper = new EhrStatusChangeMapper(canonicalObjectMapper, connectorConfig);

    }

    @Override
    public DataSource getEHRBaseDataSource() {
        return hikariDataSource;
    }

    @Override
    public OpenEHRChangeLogService getOpenEHRChangeLogService() {
        return changeLogService;
    }

    @Override
    public ObjectMapper getCanonicalObjectMapper() {
        return canonicalObjectMapper;
    }

    @Override
    public CompositionChangeMapper getCompositionChangeMapper() {
        return compositionChangeRecordMapper;
    }

    @Override
    public EhrStatusChangeMapper getEhrStatusChangeMapper() {
        return ehrStatusChangeRecordMapper;
    }

    @Override
    public PartitionOffsetFactory getPartitionOffsetFactory() {
        return partitionOffsetFactory;
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
