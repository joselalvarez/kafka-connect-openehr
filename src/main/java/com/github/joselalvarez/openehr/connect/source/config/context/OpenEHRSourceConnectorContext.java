package com.github.joselalvarez.openehr.connect.source.config.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeRecordMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeRecordMapper;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffsetFactory;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;

import javax.sql.DataSource;

public interface OpenEHRSourceConnectorContext {
    void close();
    boolean isClosed();
    DataSource getEHRBaseDataSource();
    OpenEHRChangeLogService getOpenEHREventLogService();
    ObjectMapper getCanonicalObjectMapper();
    CompositionChangeRecordMapper getCompositionChangeRecordMapper();
    EhrStatusChangeRecordMapper getEhrStatusChangeRecordMapper();
    RecordOffsetFactory getRecordOffsetFactory();
}
