package com.github.joselalvarez.openehr.connect.source.repository;

import com.github.joselalvarez.openehr.connect.source.repository.entity.CommonAggregateEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogFilter;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogOffset;
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

        public static Query buildQuery(EventLogFilter filter) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(filter.getTablePartitions());
            queryParams.add(filter.getTasks());
            queryParams.add(filter.getTaskId());

            ZonedDateTime bestFromDate = filter.getBestFromDate();
            if (bestFromDate != null)
                queryParams.add(Timestamp.from(bestFromDate.toInstant()));

            if (filter.getToDate() != null)
                queryParams.add(Timestamp.from(filter.getToDate().toInstant()));

            if (StringUtils.isNotBlank(filter.getTemplateId()))
                queryParams.add(filter.getTemplateId());

            if (StringUtils.isNotBlank(filter.getRootConcept()))
                queryParams.add(filter.getRootConcept());

            for (EventLogOffset offset : filter.getOffsetList()) {
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

            queryParams.add(filter.getBatchSize());

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

        public static Query buildQuery(EventLogFilter filter) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(filter.getTablePartitions());
            queryParams.add(filter.getTasks());
            queryParams.add(filter.getTaskId());

            ZonedDateTime bestFromDate = filter.getBestFromDate();
            if (bestFromDate != null)
                queryParams.add(Timestamp.from(bestFromDate.toInstant()));

            if (filter.getToDate() != null)
                queryParams.add(Timestamp.from(filter.getToDate().toInstant()));

            for (EventLogOffset offset : filter.getOffsetList()) {
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

            queryParams.add(filter.getBatchSize());

            SQL_TMPL.execute(writer, filter);
            log.debug(writer.toString());

            return new Query(writer.toString(),queryParams.toArray());
        }
    }


    private final QueryRunner queryRunner;
    private final CommonAggregateEvent.BeanListHandler aggregateEventHandler;

    public EHRBaseRepository(QueryRunner queryRunner) {
        this.queryRunner = queryRunner;
        this.aggregateEventHandler = new CommonAggregateEvent.BeanListHandler();
    }

    public List<CommonAggregateEvent> findCompositionAggregateEventList(EventLogFilter filter) throws SQLException {
        Query query = CompositionAggregateEventListQueryHelper.buildQuery(filter);
        return queryRunner.query(query.getSql(), aggregateEventHandler, query.getParams());
    }

    public List<CommonAggregateEvent> findEhrStatusAggregateEventList(EventLogFilter filter) throws SQLException {
        Query query = EhrStatusAggregateEventListQueryHelper.buildQuery(filter);
        return queryRunner.query(query.getSql(), aggregateEventHandler, query.getParams());
    }
}
