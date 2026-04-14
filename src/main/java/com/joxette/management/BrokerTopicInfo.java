package com.joxette.management;

public record BrokerTopicInfo(
    String topicName,
    int partitionCount,
    boolean isRecorded,
    String recordingMode   // null if not recorded
) {}
