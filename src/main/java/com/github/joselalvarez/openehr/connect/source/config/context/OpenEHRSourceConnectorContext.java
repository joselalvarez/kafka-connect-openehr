package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHREventLogService;

import javax.sql.DataSource;

public interface OpenEHRSourceConnectorContext {
    void close();
    boolean isClosed();
    DataSource getEHRBaseDataSource();
    OpenEHREventLogService getOpenEHREventLogService();
    ObjectMapper getCanonicalObjectMapper();
    CompositionEventRecordMapper geCompositionEventRecordMapper();
}
