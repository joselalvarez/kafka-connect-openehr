package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeMapper;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;

import javax.sql.DataSource;

public interface OpenEHRSourceConnectorContext {
    void close();
    boolean isClosed();
    DataSource getEHRBaseDataSource();
    OpenEHRChangeLogService getOpenEHRChangeLogService();
    ObjectMapper getCanonicalObjectMapper();
    CompositionChangeMapper getCompositionChangeMapper();
    EhrStatusChangeMapper getEhrStatusChangeMapper();
    PartitionOffsetFactory getPartitionOffsetFactory();
}
