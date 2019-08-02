/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either qexpress or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.reporting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;

// We will go back 'windowSize' minutes and obtain the information for the connection at that
// time and compare the delta's between the byte and file count values. Using that information
// we will model a line using the standard y=mx+b formula to determine when this connection
// would overflow the queue thresholds if the data continued coming in at this rate.
final class QueueOverflowMonitor {

    private static final Logger logger = LoggerFactory.getLogger(QueueOverflowMonitor.class);
    static long timeToByteOverflow;
    static long timeToCountOverflow;
    static long alertThreshold;

  static void computeOverflowEstimate(final Connection conn, final FlowController flowController) {
      logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
      logger.info(">>>> ==== Compute time to fail for Connection: " + conn.getName());
      logger.info(">>>> ==== threshold is " + flowController.getTimeToOverflowGraphThreshold());

      alertThreshold = (long) flowController.getTimeToOverflowGraphThreshold();
      timeToCountOverflow = 0; // alertThreshold;
      timeToByteOverflow = 0;  // alertThreshold;
      int offset = Math.abs(flowController.getTimeToOverflowWindowSize()) + 1;
      logger.info(">>>> offset: " + offset);

      // - get current time and determine time, 'windowSize' minutes in the past.
      Date endTime = new Date();
      Date startTime = DateUtils.addMinutes(endTime, -(offset));
      logger.info(">>>> startTime: " + startTime);
      logger.info(">>>> endTime:   " + endTime);

      // Using those times we will get the statusHistory information corresponding to the
      // oldest history up to current-windowSize and the latest history time.
      List<StatusSnapshotDTO> snapshots = flowController
          .getConnectionStatusHistory(conn.getIdentifier(), startTime, null, offset)
          .getAggregateSnapshots();

      int numberOfSnapshots = snapshots.size();

      logSnapshots(snapshots);

      // If less than 2 snapshots, set ttf to 0
      if (numberOfSnapshots < 2) {
        timeToCountOverflow = 0; //alertThreshold;
        timeToByteOverflow = 0; //alertThreshold;
        logger.info(">>>> numberOfSnapshots: " + numberOfSnapshots +"; (" + timeToByteOverflow +
            ", " + timeToCountOverflow  + ")");
        return;
      }
      // get threshold information
      long maxFiles = conn.getFlowFileQueue().getBackPressureObjectThreshold();
      String maxBytesAsString = conn.getFlowFileQueue().getBackPressureDataSizeThreshold();
      long maxBytes = (long)FormatUtils.getValueFromFormattedDataSize(maxBytesAsString);

      logger.info(">>>> Threshold Values: (" + maxBytesAsString + " - " + maxBytes + ") / " + maxFiles);

      int current = numberOfSnapshots - 1;
      int oldest = Math.max(0, numberOfSnapshots - offset);

      logger.info(">>>> snapshot indexes: " + oldest + " to " + current);

      // Would like snapshots 'window' minutes prior to calculating, so if less than 'window'
      // entries calculate with what we have until then 'window' size reached.
      StatusSnapshotDTO startSnapshot = snapshots.get(oldest);
      StatusSnapshotDTO endSnapshot = snapshots.get(current);

      String pattern = "yyyy-MM-dd hh:mm:ss";
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
      String formattedDate = simpleDateFormat.format(startSnapshot.getTimestamp());
      logger.info(">>>> oldest snapshot time: " + formattedDate);
      formattedDate = simpleDateFormat.format(endSnapshot.getTimestamp());
      logger.info(">>>> newest snapshot time: " + formattedDate);

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
    logger.info(">>>> * Retrieved Snapshots:");
    int filter = 0;
    for (StatusSnapshotDTO dto : snapshots) {
      //if (filter == 0 || filter == snapshots.size()-1) {
        logger.info(">>>> * date: " + dto.getTimestamp().toString());
        Map<String,Long> statusMetrics = dto.getStatusMetrics();
        logger.info(
            ">>>>\t* queuedCount / queuedBytes ==> " + statusMetrics.get("queuedCount") + " / " + statusMetrics.get("queuedBytes"));
      //}
      filter++;
    }
  }

    public static long diffInMinutes(Date date1, Date date2) {
      long diffInMillis = Math.abs(date2.getTime() - date1.getTime());
      return TimeUnit.MINUTES.convert(diffInMillis, TimeUnit.MILLISECONDS);
    }

    private static void computeTimeToFailureBytes(long limit, long current, long prev,
        long delta) {
        timeToByteOverflow = getTimeToOverflow(limit, current, prev, delta);
    }

    static void computeTimeToFailureFiles(long threshold, long current, long prev,
      long delta) {
      timeToCountOverflow = getTimeToOverflow(threshold, current, prev, delta);
    }

    // y = mx + b
    // max = slope * x + current_val_of_bytes_or_count
    // x represents time that current_val will reach overflow. Solve for x.
    // Slope = rise/run -> (current_val - prev_val) / time_delta (in minutes)
    static long getTimeToOverflow(Long max, long current, long prev, long delta) {
        logger.info(">>>> current / prev / delta / max");
        logger.info(">>>> " + current + " / " + prev + " / " + delta + " / " + max);

        // if MAX has been met or exceeded then set graph to 0.
        // No need for further calculation
        if (current >= max) {
          logger.info(">>>> current == threshold");
          return 0L;
        }

        // if not enough time has passed to have a delta value then set graph to 0
        if (delta <= 0L) {
          logger.info(">>>> delta == 0");
          return 0L;
        }

        // Determine slope, making sure not to divide by 0
        double slope = (current - prev) / (double) delta;
        String msg = String.format(">>>> slope: {%5.2f}", slope);
        logger.info(msg);

        // if slope is 0 or less then there is no worry of overflow happening. Decided to allow
        // user to select a threshold value at which time they would like to see graph values
        // begin tracking. Set to this value.
        if (slope <= 0) {
          return alertThreshold;
        }

        // Compute the estimated time to overflow
        double estimatedOverflow = (max - current) / slope;
        logger.info(">>>> estimatedOverflow -> {} : ({} - {}) / {}", estimatedOverflow,
            max, current, slope);

        BigDecimal bd = new BigDecimal(Double.toString(estimatedOverflow));
        estimatedOverflow = bd.setScale(0, RoundingMode.HALF_UP).doubleValue();

        long estimateAsLong = (long) (estimatedOverflow);
        logger.info(">>>> Overflow Estimate: " + estimateAsLong);
        return estimateAsLong;
    }

    static long getTimeToByteOverflow() {
      timeToByteOverflow = Math.min(timeToByteOverflow, alertThreshold);
      // return as milliseconds
      return timeToByteOverflow * 60000;
    }

    static long getTimeToCountOverflow() {
      timeToCountOverflow = Math.min(timeToCountOverflow, alertThreshold);
      // return as milliseconds
      return timeToCountOverflow * 60000;
    }
}

