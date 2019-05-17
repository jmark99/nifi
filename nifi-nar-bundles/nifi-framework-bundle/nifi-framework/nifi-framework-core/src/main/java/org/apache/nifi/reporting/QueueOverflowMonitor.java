package org.apache.nifi.reporting;

import org.apache.nifi.connectable.Connection;

public final class QueueOverflowMonitor {

    private static long timeToByteOverflow;
    private static long timeToCountOverflow;

    private QueueOverflowMonitor() {}

    public static void computeOverflowEstimate(Connection conn) {
      timeToByteOverflow = 3L;
      timeToCountOverflow = 5L;
    }

    public static long getTimeToByteOverflow() {
      return timeToByteOverflow;
    }

    public static long getTimeToCountOverflow() {
      return timeToCountOverflow;
    }
}

/*
    private long [] computeTimeToFailure(final Connection conn) {
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        logger.info(">>>> COMPUTE TTF for Connection : " + conn.getIdentifier());
        long [] ttf = new long[2];

        int ALERT_THRESHOLD_MINUTES;

        try {
            if (properties == null) {
                logger.info(">>>> properties IS NULL");
            }
            logger.info(">>>> STATUS_HISTORY_ALERT_THRESHOLD: " + NiFiProperties.STATUS_HISTORY_ALERT_THRESHOLD);
            ALERT_THRESHOLD_MINUTES = properties.getStatusHistoryThresholdAlert();
        } catch (Exception ex) {
            logger.info(">>>> EXCEPTION THROWN getting threshold alert value: " + ex.getMessage());
            ALERT_THRESHOLD_MINUTES = 240;
        }
        logger.info(">>>> ALERT_THRESHOLD_MINUTES: " + ALERT_THRESHOLD_MINUTES);

        // = properties.getStatusHistoryThresholdAlert(); //240;

        // TODO use a user provided value as the default if there is no fear of overflow
        // Use 12 hours for now
        ttf[0] = ALERT_THRESHOLD_MINUTES * 60 * 1000;
        ttf[1] = ALERT_THRESHOLD_MINUTES * 60 * 1000;

        final int HISTORY = -5; // minutes
        final int HISTORY_OFFSET = Math.abs(HISTORY) + 1;
        // will need the stats for the connection 15 minutes prior as well as stats for latest
        // connection.
        Date currentTime = new Date();
        Date previousTime = DateUtils.addMinutes(currentTime, HISTORY - 2);
        logger.info(">>>> prevTime:    " + previousTime.toString());
        logger.info(">>>> currentTime: " + currentTime.toString());
        // These dates are used to get the connection history for that time interval.
        // Shouldn't need more than HISTORY_OFFSET

        List<StatusSnapshotDTO> aggregateSnapshots = flowController
            .getConnectionStatusHistory(conn.getIdentifier(), previousTime, null, HISTORY_OFFSET)
            .getAggregateSnapshots();

//        StatusHistoryDTO connHistory =
//            flowController.getConnectionStatusHistory(conn.getIdentifier(), previousTime,
//                null, TIME_OFFSET);
//        // get a list of the snapshot data
//        List<StatusSnapshotDTO> aggregateSnapshots = connHistory.getAggregateSnapshots();

        int numberOfSnapshots = aggregateSnapshots.size();
        logger.info(">>>> aggregateSnapshots.length: " + numberOfSnapshots);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        for (StatusSnapshotDTO dto : aggregateSnapshots) {
            logger.info(">>>> Date: " + dto.getTimestamp().toString());
            Map<String,Long> statusMetrics = dto.getStatusMetrics();
            logger.info(">>>> metrics: " + statusMetrics.toString());
        }
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        if (numberOfSnapshots < HISTORY_OFFSET) {
            logger.info(">>>> Not enough snapshots yet (" + numberOfSnapshots + ")...return "
                + "default values: " + ttf[0]);
            return ttf;
        }

        // Grab needed data for all calculations
        Long countThreshold =
            Long.valueOf(conn.getFlowFileQueue().getBackPressureObjectThreshold());
        String backPressureDataSizeThreshold = conn.getFlowFileQueue()
            .getBackPressureDataSizeThreshold();
        Long bytesThreshold = convertThresholdToBytes(backPressureDataSizeThreshold);

        logger.info(">>>> countThreshold: " + countThreshold);
        logger.info(">>>> backPressureDataSizeThreshold: " + backPressureDataSizeThreshold);
        logger.info(">>>> bytesThreshold: " + bytesThreshold);

        // Would like HISTORY minutes prior to calculating, so if less than HISTORY entries
        // calculate with what we have until then.
        int current = numberOfSnapshots - 1;
        int oldest = Math.max(0, numberOfSnapshots - HISTORY_OFFSET);

        StatusSnapshotDTO oldestSnapshot = aggregateSnapshots.get(oldest);
        logger.info(">>>> Oldest Date: " + oldestSnapshot.getTimestamp().toString());
        StatusSnapshotDTO currentSnapshot = aggregateSnapshots.get(current);
        logger.info(">>>> Current Date: " + currentSnapshot.getTimestamp().toString());

        long currentCount = currentSnapshot.getStatusMetrics().get("queuedCount");
        long oldestCount = oldestSnapshot.getStatusMetrics().get("queuedCount");
        logger.info(">>>> currentCount / prevCount: " + currentCount + " / " + oldestCount);

        long currentBytes = currentSnapshot.getStatusMetrics().get("queuedBytes");
        long oldestBytes = oldestSnapshot.getStatusMetrics().get("queuedBytes");
        logger.info(">>>> currentBytes / prevBytes: " + currentBytes + " / " + oldestBytes);

        long timeDeltaInMinutes = diffInMinutes(oldestSnapshot.getTimestamp(),
            currentSnapshot.getTimestamp(), TimeUnit.MINUTES);
        logger.info(">>>> delta: " + timeDeltaInMinutes);

        if (timeDeltaInMinutes < 1) {
            logger.info(">>>> time delta still 0...return default values");
            return ttf;
        }

        long rawTTFCount = computeTimeToFailureCount(countThreshold, currentCount, oldestCount,
            timeDeltaInMinutes);
        long rawTTFBytes = computeTimeToFailureBytes(bytesThreshold, currentBytes, oldestBytes,
            timeDeltaInMinutes);
        logger.info(">>>> rawTTFCount: " + rawTTFCount);
        logger.info(">>>> rawTTFCount: " + rawTTFBytes);

        long ttfCountInMillis = rawTTFCount * 60 * 1000;
        long ttfBytesInMillis = rawTTFBytes * 60 * 1000;

        logger.info(">>>> ttfCountInMillis: " + ttfCountInMillis);
        logger.info(">>>> ttfBytesInMillis: " + ttfBytesInMillis);

        if (currentCount == countThreshold) {
            ttf[0] = 0;
        } else {
            ttf[0] = Math.min(ttfCountInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
        }

        if (currentBytes == bytesThreshold) {
            ttf[1] = 0;
        } else {
            ttf[1] = Math.min(ttfBytesInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
        }

        //ttf[0] = Math.min(ttfCountInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
        //ttf[1] = Math.min(ttfBytesInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);

        logger.info(">>>> predicted count ttf: " + ttf[0] );
        logger.info(">>>> predicted byte  ttf: " + ttf[1]);

        return ttf;
    }

    private long convertThresholdToBytes(String backPressureDataSizeThreshold) {
        final long BYTES_IN_KILOBYTE = 1024L;
        final long BYTES_IN_MEGABYTE = 1048576L;
        final long BYTES_IN_GIGABYTE = 1073741824L;
        final long BYTES_IN_TERABYTE = 1099511627776L;
        long bytes;

        String[] threshold = backPressureDataSizeThreshold.split("\\s+");
        if (threshold[1].toLowerCase().contains("tb")) {
            bytes = Long.valueOf(threshold[0]) * BYTES_IN_TERABYTE;
        } else if (threshold[1].toLowerCase().contains("gb")) {
            bytes = Long.valueOf(threshold[0]) * BYTES_IN_GIGABYTE;
        } else if (threshold[1].toLowerCase().contains("mb")) {
            bytes = Long.valueOf(threshold[0]) * BYTES_IN_MEGABYTE;
        } else if (threshold[1].toLowerCase().contains("kb")) {
            bytes = Long.valueOf(threshold[0]) * BYTES_IN_KILOBYTE;
        } else {
            bytes = Long.valueOf(threshold[0]);
        }
        return bytes;
    }

    private long computeTimeToFailureCount(Long countThreshold, long current, long prev,
        long delta) {
        final int ALERT_THRESHOLD_MINUTES = 240;
        // y = mx + b
        // y = countThreshold
        // b = currentCount
        // m = (current_val - prev_val) / time_delta
        double slope = (current - prev) / (double)delta;
        logger.info(">>>> countSlope: " + slope);
        double dttfCount;
        if (slope <= 0) {
          logger.info(">>>> slope is 0 or negative (" + slope + ")...");
            dttfCount = ALERT_THRESHOLD_MINUTES;
        } else {
            dttfCount = (countThreshold - current) / slope;
        }
        long ttfCount = (long)dttfCount;
        return ttfCount;
    }

    private long computeTimeToFailureBytes(Long bytesThreshold, long current, long prev,
        long delta) {
        final int ALERT_THRESHOLD_MINUTES = 240;
        // y = mx + b
        // y = countThreshold
        // b = currentCount
        // m = (current_val - prev_val) / time_delta
        double slope = (current - prev) / (double)delta;
        logger.info(">>>> bytesSlope: " + slope);
        double dttfBytes;
        if (slope <= 0) {
            logger.info(">>>> slope is 0 or negative (" + slope + ")...");
            dttfBytes = ALERT_THRESHOLD_MINUTES;
        } else {
            dttfBytes = (bytesThreshold - current) / slope;
        }
        long ttfBytes = (long)dttfBytes;
        return ttfBytes;
    }

    private long computeTimeToFailure(Long threshold, long current, long prev, long delta) {

    }


    private long diffInMinutes(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillis = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillis, TimeUnit.MILLISECONDS);
    }
 */