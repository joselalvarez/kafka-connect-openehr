package com.github.joselalvarez.openehr.connect.source.ehrbase.entity;

import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@ToString
public class EHRBaseChangePartitionOffset implements PartitionOffset {

    private static String TYPE_TAG = "t";
    private static String PARTITION_TAG = "p";

    private static String DATE_TAG = "d";
    private static String UID_TAG = "u";
    private static String VERSION_TAG = "v";

    private String type;
    private int partition;
    private ZonedDateTime date;
    private UUID uid;
    private Integer version;

    private EHRBaseChangePartitionOffset(String type, int partition, ZonedDateTime date, UUID uid, Integer version){
        this.type = type;
        this.partition = partition;
        this.date = date;
        this.uid = uid;
        this.version = version;
    }

    public EHRBaseChangePartitionOffset(Class<?> type, int partition, ZonedDateTime date, UUID uid, Integer version){
        this(type != null ? type.getSimpleName() : null, partition, date, uid, version);
    }

    public EHRBaseChangePartitionOffset(Class<?> type, int partition) {
        this(type, partition, null, null ,null);
    }

    public boolean isEmpty() {
        return date == null || uid == null || version == null;
    }

    public static EHRBaseChangePartitionOffset from(Map<String, ?> partition, Map<String, ?> offset) {
        return new EHRBaseChangePartitionOffset(
                partition.get(TYPE_TAG) != null ? partition.get(TYPE_TAG).toString() : null,
                Integer.valueOf(partition.get(PARTITION_TAG).toString()),
                offset.get(DATE_TAG) != null ? ZonedDateTime.parse(offset.get(DATE_TAG).toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null,
                offset.get(UID_TAG) != null ? UUID.fromString((String) offset.get(UID_TAG)) : null,
                offset.get(VERSION_TAG) != null ? Integer.valueOf((String) offset.get(VERSION_TAG)) : null
        );
    }

    public static EHRBaseChangePartitionOffset from(PartitionOffset offset) {
        return offset != null ? from(offset.getPartitionMap(), offset.getOffsetMap()) : null;
    }

    @Override
    public Map<String, ?> getOffsetMap() {
        Map<String, String> map = new LinkedHashMap<>();
        if (!isEmpty()) {
            map.put(DATE_TAG, date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            map.put(UID_TAG, uid.toString());
            map.put(VERSION_TAG, version.toString());
        }
        return Collections.unmodifiableMap(map);

    }

    @Override
    public Map<String, ?> getPartitionMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(TYPE_TAG, StringUtils.isNotBlank(type) ? type : null);
        map.put(PARTITION_TAG, String.valueOf(partition));
        return Collections.unmodifiableMap(map);
    }

    @Override
    public String toString() {
        return String.format("type=%s, partition=%s, time_commit=%s, uid=%s, version=%s", type, partition, date, uid, version);
    }

}
