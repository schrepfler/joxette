package com.joxette.management;

public record BrokerTopicInfo(
    String topicName,
    int partitionCount,
    boolean isRecorded,
    TopicMode recordingMode   // null if not recorded
) {}
