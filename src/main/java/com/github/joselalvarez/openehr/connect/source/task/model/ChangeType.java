package com.github.joselalvarez.openehr.connect.source.task.model;

import org.apache.commons.lang3.StringUtils;

public enum ChangeType {

    CREATION, MODIFICATION, DELETED;

    public static ChangeType parse(String value) {
        return StringUtils.isNotBlank(value) ? ChangeType.valueOf(value.trim().toUpperCase()) : null;
    }

    public String getValue() {
        return name().toLowerCase();
    }

}
