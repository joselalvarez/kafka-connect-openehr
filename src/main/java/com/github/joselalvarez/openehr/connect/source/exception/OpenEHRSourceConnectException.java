package com.github.joselalvarez.openehr.connect.source.exception;

import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import org.apache.kafka.connect.errors.ConnectException;

public class OpenEHRSourceConnectException extends ConnectException {

    public OpenEHRSourceConnectException(String message, Throwable cause) {
        super(message, cause);
    }

    public static OpenEHRSourceConnectException repositoryError(Throwable throwable) {
        return new OpenEHRSourceConnectException("An error occurred while reading data from the OpenEHR repository", throwable);
    }

    public static OpenEHRSourceConnectException serdeError(Throwable throwable) {
        return new OpenEHRSourceConnectException("An error occurred while serializing/deserializing data", throwable);
    }

    public static OpenEHRSourceConnectException mappingError(Throwable throwable, PartitionOffset partitionOffset) {
        return new OpenEHRSourceConnectException(String.format("An error occurred while mapping partition offset data: %s", partitionOffset.toString()), throwable);
    }
}
