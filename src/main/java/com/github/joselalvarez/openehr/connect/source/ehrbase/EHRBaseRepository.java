package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseEvent;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseEventOffset;
import com.github.joselalvarez.openehr.connect.source.record.RecordOffset;
import com.github.joselalvarez.openehr.connect.source.task.model.EventFilter;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;

import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EHRBaseRepository {

    @Getter
    @AllArgsConstructor
    private static class Query {
        private String sql;
        private Object [] params;
    }

    private static final MustacheFactory mustacheFactory = new DefaultMustacheFactory();

    private static class CompositionAggregateEventListQueryHelper {

        private static final String SQL_TMPL_PATH = "sql/find-composition-agg-event-list.sql";
        private static final Mustache SQL_TMPL;

        static {
            SQL_TMPL = mustacheFactory.compile(SQL_TMPL_PATH);
        }

        public static Query buildQuery(EventFilter filter, int tablePartitionSize, long pollSize) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(tablePartitionSize);

            ZonedDateTime bestFromDate = null;//filter.getBestFromDate();
            if (bestFromDate != null)
                queryParams.add(Timestamp.from(bestFromDate.toInstant()));

            if (filter.getToDate() != null)
                queryParams.add(Timestamp.from(filter.getToDate().toInstant()));

            if (StringUtils.isNotBlank(filter.getTemplateId()))
                queryParams.add(filter.getTemplateId());

            if (StringUtils.isNotBlank(filter.getRootConcept()))
                queryParams.add(filter.getRootConcept());

            for (RecordOffset o : filter.getOffsetList()) {
                EHRBaseEventOffset offset = EHRBaseEventOffset.from(o);
                queryParams.add(offset.getPartition());
                if (!offset.isEmpty()) {
                    Timestamp t = Timestamp.from(offset.getDate().toInstant());
                    queryParams.add(t);
                    queryParams.add(t);
                    queryParams.add(offset.getUid());
                    queryParams.add(t);
                    queryParams.add(offset.getUid());
                    queryParams.add(offset.getVersion());
                }
            }

            queryParams.add(pollSize);

            SQL_TMPL.execute(writer, filter);
            log.debug(writer.toString());

            return new Query(writer.toString(),queryParams.toArray());
        }
    }

    private static class EhrStatusAggregateEventListQueryHelper {

        private static final String SQL_TMPL_PATH = "sql/find-ehr-status-agg-event-list.sql";
        private static final Mustache SQL_TMPL;

        static {
            SQL_TMPL = mustacheFactory.compile(SQL_TMPL_PATH);
        }

        public static Query buildQuery(EventFilter filter, int tablePartitionSize, long pollSize) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(tablePartitionSize);

            ZonedDateTime bestFromDate = null;//filter.getBestFromDate();
            if (bestFromDate != null)
                queryParams.add(Timestamp.from(bestFromDate.toInstant()));

            if (filter.getToDate() != null)
                queryParams.add(Timestamp.from(filter.getToDate().toInstant()));

            for (RecordOffset o : filter.getOffsetList()) {
                EHRBaseEventOffset offset = EHRBaseEventOffset.from(o);
                queryParams.add(offset.getPartition());
                if (!offset.isEmpty()) {
                    Timestamp t = Timestamp.from(offset.getDate().toInstant());
                    queryParams.add(t);
                    queryParams.add(t);
                    queryParams.add(offset.getUid());
                    queryParams.add(t);
                    queryParams.add(offset.getUid());
                    queryParams.add(offset.getVersion());
                }
            }

            queryParams.add(pollSize);

            SQL_TMPL.execute(writer, filter);
            log.debug(writer.toString());

            return new Query(writer.toString(),queryParams.toArray());
        }
    }


    private final OpenEHRSourceConnectorConfig connectorConfig;
    private final QueryRunner queryRunner;
    private final EHRBaseEventOffsetFactory offsetFactory;
    private final EHRBaseEvent.BeanListHandler aggregateEventHandler;

    public EHRBaseRepository(OpenEHRSourceConnectorConfig connectorConfig, QueryRunner queryRunner, EHRBaseEventOffsetFactory offsetFactory) {
        this.connectorConfig = connectorConfig;
        this.queryRunner = queryRunner;
        this.offsetFactory = offsetFactory;
        this.aggregateEventHandler = new EHRBaseEvent.BeanListHandler();
    }

    public List<EHRBaseEvent> findCompositionEventList(EventFilter filter) throws SQLException {
        Query query = CompositionAggregateEventListQueryHelper.buildQuery(filter, connectorConfig.getTablePartitionSize(), connectorConfig.getPollBatchSize());
        return queryRunner.query(query.getSql(), aggregateEventHandler, query.getParams());
    }

    public List<EHRBaseEvent> findEhrStatusEventList(EventFilter filter) throws SQLException {
        Query query = EhrStatusAggregateEventListQueryHelper.buildQuery(filter, connectorConfig.getTablePartitionSize(), connectorConfig.getPollBatchSize());
        return queryRunner.query(query.getSql(), aggregateEventHandler, query.getParams());
    }
}
