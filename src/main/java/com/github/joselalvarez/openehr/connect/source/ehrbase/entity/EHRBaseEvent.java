package com.github.joselalvarez.openehr.connect.source.ehrbase.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
public class EHRBaseEvent {

    private int tablePartition;

    private UUID uid;
    private UUID ehrId;
    private String changeType;
    private ZonedDateTime timeCommitted;
    private Integer sysVersion;
    private byte [] aggregate;

    public static class BeanListHandler implements ResultSetHandler<List<EHRBaseEvent>> {

        private static final ObjectMapper objectMapper = new ObjectMapper();

        public static final String TABLE_PARTITION = "table_partition";
        public static final String UID = "uid";
        public static final String EHR_ID = "ehr_id";
        public static final String CHANGE_TYPE = "change_type";
        public static final String TIME_COMMITTED = "time_committed";
        public static final String SYS_VERSION = "sys_version";
        public static final String AGGREGATE = "aggregate";

        public EHRBaseEvent map(EHRBaseEvent target, ResultSet source) throws SQLException {
            target.setTablePartition(source.getInt(TABLE_PARTITION));
            String uid = source.getString(UID);
            target.setUid(StringUtils.isNotBlank(uid) ? UUID.fromString(uid) : null);
            String ehrId = source.getString(EHR_ID);
            target.setEhrId(StringUtils.isNotBlank(ehrId) ? UUID.fromString(ehrId) : null);
            target.setChangeType(source.getString(CHANGE_TYPE));
            Timestamp timeCommitted = source.getTimestamp(TIME_COMMITTED);
            target.setTimeCommitted(timeCommitted != null ? timeCommitted.toInstant().atZone(ZoneId.systemDefault()) : null);
            target.setSysVersion(source.getObject(SYS_VERSION, Integer.class));
            target.setAggregate(source.getBytes(AGGREGATE));
            return target;
        }

        @Override
        public List<EHRBaseEvent> handle(ResultSet rs) throws SQLException {
            List<EHRBaseEvent> resultList = new ArrayList<>();
            while (rs.next()){
                resultList.add(map(new EHRBaseEvent(), rs));
            }
            return resultList;
        }

    }
}
