package com.github.joselalvarez.openehr.connect.source.service.model;

import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.UIDBasedId;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
public class EhrStatusEvent {

    private ChangeType changeType;
    private ZonedDateTime timeCommitted;

    private UUID ehrId;
    private UUID uid;
    private Integer version;

    private ArchetypeID archetypeID;
    private UIDBasedId ehrStatusId;
    private UIDBasedId replacedId;
    private EhrStatus ehrStatus;

    private EventLogOffset offset;

}
