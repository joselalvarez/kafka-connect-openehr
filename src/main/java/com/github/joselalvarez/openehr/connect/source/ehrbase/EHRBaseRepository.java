package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChange;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChangePartitionOffset;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import com.github.joselalvarez.openehr.connect.source.service.model.ChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChangeRequest;
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

        private static final String SQL_TMPL_PATH = "sql/find-composition-change-agg-list.sql";
        private static final Mustache SQL_TMPL;

        static {
            SQL_TMPL = mustacheFactory.compile(SQL_TMPL_PATH);
        }

        public static Query buildQuery(CompositionChangeRequest request, int tablePartitionSize) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(tablePartitionSize);

            if (request.getFromDate() != null)
                queryParams.add(Timestamp.from(request.getFromDate().toInstant()));

            if (request.getToDate() != null)
                queryParams.add(Timestamp.from(request.getToDate().toInstant()));

            if (StringUtils.isNotBlank(request.getTemplateId()))
                queryParams.add(request.getTemplateId());

            if (StringUtils.isNotBlank(request.getRootConcept()))
                queryParams.add(request.getRootConcept());

            for (PartitionOffset o : request.getPartitionOffsets()) {
                EHRBaseChangePartitionOffset partitionOffset = EHRBaseChangePartitionOffset.from(o);
                queryParams.add(partitionOffset.getPartition());
                if (!partitionOffset.isEmpty()) {
                    Timestamp t = Timestamp.from(partitionOffset.getDate().toInstant());
                    queryParams.add(t);
                    queryParams.add(t);
                    queryParams.add(partitionOffset.getUid());
                    queryParams.add(t);
                    queryParams.add(partitionOffset.getUid());
                    queryParams.add(partitionOffset.getVersion());
                }
            }

            queryParams.add(request.getMaxPoll());

            SQL_TMPL.execute(writer, request);
            log.debug(writer.toString());

            return new Query(writer.toString(),queryParams.toArray());
        }
    }

    private static class EhrStatusAggregateEventListQueryHelper {

        private static final String SQL_TMPL_PATH = "sql/find-ehr-status-change-agg-list.sql";
        private static final Mustache SQL_TMPL;

        static {
            SQL_TMPL = mustacheFactory.compile(SQL_TMPL_PATH);
        }

        public static Query buildQuery(ChangeRequest request, int tablePartitionSize) {
            StringWriter writer = new StringWriter();
            List<Object> queryParams = new ArrayList<>();

            queryParams.add(tablePartitionSize);

            if (request.getFromDate() != null)
                queryParams.add(Timestamp.from(request.getFromDate().toInstant()));

            if (request.getToDate() != null)
                queryParams.add(Timestamp.from(request.getToDate().toInstant()));

            for (PartitionOffset o : request.getPartitionOffsets()) {
                EHRBaseChangePartitionOffset partitionOffset = EHRBaseChangePartitionOffset.from(o);
                queryParams.add(partitionOffset.getPartition());
                if (!partitionOffset.isEmpty()) {
                    Timestamp t = Timestamp.from(partitionOffset.getDate().toInstant());
                    queryParams.add(t);
                    queryParams.add(t);
                    queryParams.add(partitionOffset.getUid());
                    queryParams.add(t);
                    queryParams.add(partitionOffset.getUid());
                    queryParams.add(partitionOffset.getVersion());
                }
            }

            queryParams.add(request.getMaxPoll());

            SQL_TMPL.execute(writer, request);
            log.debug(writer.toString());

            return new Query(writer.toString(),queryParams.toArray());
        }
    }

    private final OpenEHRSourceConnectorConfig connectorConfig;
    private final QueryRunner queryRunner;
    private final EHRBaseChange.BeanListHandler beanListHandler;

    public EHRBaseRepository(OpenEHRSourceConnectorConfig connectorConfig, QueryRunner queryRunner) {
        this.connectorConfig = connectorConfig;
        this.queryRunner = queryRunner;
        this.beanListHandler = new EHRBaseChange.BeanListHandler();
    }

    public List<EHRBaseChange> findCompositionChanges(CompositionChangeRequest request) throws SQLException {
        Query query = CompositionAggregateEventListQueryHelper.buildQuery(request, connectorConfig.getTablePartitionSize());
        return queryRunner.query(query.getSql(), beanListHandler, query.getParams());
    }

    public List<EHRBaseChange> findEhrStatusChanges(ChangeRequest request) throws SQLException {
        Query query = EhrStatusAggregateEventListQueryHelper.buildQuery(request, connectorConfig.getTablePartitionSize());
        return queryRunner.query(query.getSql(), beanListHandler, query.getParams());
    }
}
