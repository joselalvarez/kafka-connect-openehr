package com.github.joselalvarez.openehr.connect.source.service.model;

import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.support.identification.UIDBasedId;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
public class EhrStatusChange {

    private ChangeType changeType;
    private ZonedDateTime timeCommitted;

    private UUID ehrId;
    private UUID uid;
    private Integer version;

    private String archetypeId;
    private UIDBasedId ehrStatusId;
    private UIDBasedId replacedId;

    private String subjectType;
    private String subjectNamespace;
    private String subjectId;
    private String subjectIdScheme;

    private EhrStatus ehrStatus;

    private PartitionOffset partitionOffset;

}
