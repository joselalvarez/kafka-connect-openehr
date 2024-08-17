package com.github.joselalvarez.openehr.connect.common;

import com.nedap.archie.rm.support.identification.ArchetypeID;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public abstract class ConnectorConfigValidators {

    public static class IsoOffsetDateTimeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value != null && StringUtils.isNotBlank(value.toString())) {
                try {
                    ZonedDateTime.parse(value.toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (Exception e) {
                    throw new ConfigException(name, value.toString(), "entry must be a valid 'ISO_OFFSET_DATE_TIME' string");
                }
            }
        }
    }

    public static class ArchetypeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value != null && StringUtils.isNotBlank(value.toString())) {
                try {
                    new ArchetypeID(value.toString());
                } catch (Exception e) {
                    throw new ConfigException(name, value.toString(), "entry must be a valid archetype human readable id");
                }
            }
        }
    }
}
