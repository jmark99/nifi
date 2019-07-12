package org.apache.nifi.reporting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;

// We will go back 'windowSize' minutes and obtain the information for the connection at that
// time and compare the delta's between the byte and file count values. Using that information
// we will model a line using the standard y=mx+b formula to determine when this connection
// would overflow the queue thresholds if the data continued coming in at this rate.
final class QueueOverflowMonitor {

    private static final Logger logger = LoggerFactory.getLogger(QueueOverflowMonitor.class);
    private static long timeToByteOverflow;
    private static long timeToCountOverflow;
    private static long alertThreshold;

  static void computeOverflowEstimate(final Connection conn, final int threshold, final int window,
      final FlowController flowController) {
      logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
      logger.info(">>>> Compute time to fail for Connection: " + conn.getName());

      alertThreshold = (long) threshold;
      timeToCountOverflow = alertThreshold;
      timeToByteOverflow = alertThreshold;
      int offset = Math.abs(window) + 1;

      // - get current time and determine time, 'windowSize' minutes in the past.
      Date endTime = new Date();
      Date startTime = DateUtils.addMinutes(endTime, -offset);

      //String pattern = "yyyy-MM-dd hh:mm:ss";
      //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

      // Using those times we will get the statusHistory information corresponding to the
      // oldest history up to current-windowSize and the latest history time.
      List<StatusSnapshotDTO> snapshots = flowController
          .getConnectionStatusHistory(conn.getIdentifier(), startTime, null, offset)
          .getAggregateSnapshots();

      int numberOfSnapshots = snapshots.size();

      // If less than 2 snapshots, set ttf to alertThreshold
      if (numberOfSnapshots < 2) {
        timeToCountOverflow = alertThreshold;
        timeToByteOverflow = alertThreshold;
        //logger.info(">>>> numberOfSnapshots: " + numberOfSnapshots +"; (" + timeToByteOverflow +
        //    ", " + timeToCountOverflow  + ")");
        return;
      }
      // get threshold information
      long maxFiles = conn.getFlowFileQueue().getBackPressureObjectThreshold();
      String maxBytesAsString = conn.getFlowFileQueue().getBackPressureDataSizeThreshold();
      long maxBytes = convertThresholdToBytes(maxBytesAsString);

      logger.info(">>>> Threshold Values: (" + maxBytesAsString + " - " + maxBytes + ") / " + maxFiles);

      int current = numberOfSnapshots - 1;
      int oldest = Math.max(0, numberOfSnapshots - offset);

      //logger.info(">>>> snapshot indexes: " + oldest + " to " + current);

      // Would like snapshots 'window' minutes prior to calculating, so if less than 'window'
      // entries calculate with what we have until then 'window' size reached.
      StatusSnapshotDTO startSnapshot = snapshots.get(oldest);
      StatusSnapshotDTO endSnapshot = snapshots.get(current);

      //String formattedDate = simpleDateFormat.format(startSnapshot.getTimestamp());
      //logger.info(">>>> oldest snapshot time: " + formattedDate);
      //formattedDate = simpleDateFormat.format(endSnapshot.getTimestamp());
      //logger.info(">>>> newest snapshot time: " + formattedDate);

      long prevBytes = startSnapshot.getStatusMetrics().get("queuedBytes");
      long currentBytes = endSnapshot.getStatusMetrics().get("queuedBytes");
      long prevCount = startSnapshot.getStatusMetrics().get("queuedCount");
      long currentCount = endSnapshot.getStatusMetrics().get("queuedCount");

      // determine current timeDelta.
      long timeDeltaInMinutes = diffInMinutes(startSnapshot.getTimestamp(),
          endSnapshot.getTimestamp());

      if (timeDeltaInMinutes < 1) {
        return;
      }
      computeTimeToFailureBytes(maxBytes, currentBytes, prevBytes, timeDeltaInMinutes);
      computeTimeToFailureFiles(maxFiles, currentCount, prevCount, timeDeltaInMinutes);
    }

  private static void logSnapshots(List<StatusSnapshotDTO> snapshots) {
    logger.info(">>>> Retrieved Snapshots:");
    int filter = 0;
    for (StatusSnapshotDTO dto : snapshots) {
      if (filter == 0 || filter == snapshots.size()-1) {
        logger.info(">>>> date: " + dto.getTimestamp().toString());
        Map<String,Long> statusMetrics = dto.getStatusMetrics();
        logger.info(
            ">>>>\tqueuedCount / queuedBytes ==> " + statusMetrics.get("queuedCount") + " / " + statusMetrics.get("queuedBytes"));
      }
      filter++;
    }
  }

  private static long convertThresholdToBytes(String backPressureDataSizeThreshold) {
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

    private static long diffInMinutes(Date date1, Date date2) {
      long diffInMillis = date2.getTime() - date1.getTime();
      return TimeUnit.MINUTES.convert(diffInMillis, TimeUnit.MILLISECONDS);
    }

    private static void computeTimeToFailureBytes(long limit, long current, long prev,
        long delta) {
        timeToByteOverflow = computeTimeToFailure(limit, current, prev, delta);
    }

    private static void computeTimeToFailureFiles(long threshold, long current, long prev,
      long delta) {
      timeToCountOverflow = computeTimeToFailure(threshold, current, prev, delta);
  }

    // y = mx + b
    // m = slope --> rise/run --> (current_val - prev_val) / time_delta (in minutes)
    // y = overflow limit
    // b = current value of bytes/count
    // solve for x
    private static long computeTimeToFailure(Long overflowLimit, long current, long prev, long delta) {
        //logger.info(">>>> ----> (prev / current) -> (" + prev + " / " + current + "), time delta:"
        //    + " " + delta);

        // if the threshold has been met or exceeded then set graph to 0.
        if (current >= overflowLimit) {
          //logger.info(">>>> current == threshold");
          return 0L;
        }

        // make sure not dividing by zero
        if (delta <= 0L) {
          //logger.info(">>>> delta == 0, setting to alertThreshold");
          return alertThreshold;
        }

        double slope = (current - prev) / (double)delta;
        //logger.info(">>>> slope: " + slope);

        if (slope <= 0) {
          return alertThreshold;
        }

        double ttf = (overflowLimit - current) / slope;
        //logger.info(">>>> ttf -> " + ttf + " = (" + overflowLimit + " - " + current + ") / " +
        //  slope);

        BigDecimal bd = new BigDecimal(Double.toString(ttf));
        ttf = bd.setScale(0, RoundingMode.HALF_UP).doubleValue();

        long ttfAsLong = (long) (ttf);
        logger.info(">>>> computed ttf " + ttfAsLong);
        return ttfAsLong;
    }

    static long getTimeToByteOverflow() {
      timeToByteOverflow = Math.min(timeToByteOverflow, alertThreshold);
      // return as milliseconds
      return timeToByteOverflow * 1000 * 60;
    }

    static long getTimeToCountOverflow() {
      timeToCountOverflow = Math.min(timeToCountOverflow, alertThreshold);
      // return as milliseconds
      return timeToCountOverflow * 1000 * 60;
    }
}

