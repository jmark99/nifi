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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.reporting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;

// Go back 'windowSize' minutes and obtain the information for the connection at that
// time and compare the delta's between the byte and file count values. Using that information
// model a line using the standard y=mx+b formula to determine when this connection
// would overflow the queue thresholds if the data continued coming in at that rate.
final class QueueOverflowMonitor {

  private static final Logger logger = LoggerFactory.getLogger(QueueOverflowMonitor.class);

  private static long timeToByteOverflow;
  private static long timeToCountOverflow;
  private static long alertThreshold;

  static void computeOverflowEstimate(final Connection conn, final FlowController flowController) {

    alertThreshold = flowController.getTimeToOverflowGraphThreshold() * 60000;
    timeToCountOverflow = 0;
    timeToByteOverflow = 0;
    int offset = Math.abs(flowController.getTimeToOverflowWindowSize()) + 1;

    // Using those times we will get the statusHistory information corresponding to the
    // oldest history up to current-windowSize and the latest history time.
    List<StatusSnapshotDTO> snapshots = flowController
        .getConnectionStatusHistory(conn.getIdentifier(), null, null, offset)
        .getAggregateSnapshots();

    int numberOfSnapshots = snapshots.size();

    // If less than 2 snapshots cannot create an estimate.
    if (numberOfSnapshots < 2) {
      return;
    }

    // get threshold information
    long maxFiles = conn.getFlowFileQueue().getBackPressureObjectThreshold();
    String maxBytesAsString = conn.getFlowFileQueue().getBackPressureDataSizeThreshold();
    long maxBytes = (long) FormatUtils.getValueFromFormattedDataSize(maxBytesAsString);

    int current = numberOfSnapshots - 1;
    int oldest = Math.max(0, numberOfSnapshots - offset);

    // Would like snapshots 'window' minutes prior to calculating, so if less than 'window'
    // entries calculate with what we have until then 'window' size reached.
    StatusSnapshotDTO startSnapshot = snapshots.get(oldest);
    StatusSnapshotDTO endSnapshot = snapshots.get(current);

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

  // Given two date objects determine diff in minutes.
  static long diffInMinutes(Date date1, Date date2) {
    long diffInMillis = Math.abs(date2.getTime() - date1.getTime());
    return TimeUnit.MINUTES.convert(diffInMillis, TimeUnit.MILLISECONDS);
  }

  // compute time until FlowFile data size reaches overflow of declared queue limit
  private static void computeTimeToFailureBytes(long limit, long current, long prev, long delta) {
    timeToByteOverflow = getTimeToOverflow(limit, current, prev, delta);
  }

  // compute time until the FlowFile count reaches the declared FlowFile count limit.
  private static void computeTimeToFailureFiles(long threshold, long current, long prev, long delta) {
    timeToCountOverflow = getTimeToOverflow(threshold, current, prev, delta);
  }

  static long getTimeToOverflow(Long max, long current, long prev, long delta) {
    logger.trace(">>>> current / prev / delta / max");
    logger.trace(">>>> " + current + " / " + prev + " / " + delta + " / " + max);

    // if 'max' has been met or exceeded then set graph to 0.
    // No need for further calculation
    if (current >= max) {
      return 0L;
    }

    // if not enough time has passed to have a delta value then set graph to alertThreshold
    if (delta <= 0) {
      return alertThreshold;
    }

    // Determine slope
    double slope = (current - prev) / (double) delta;
    slope = BigDecimal.valueOf(slope).setScale(2, RoundingMode.HALF_UP).doubleValue();

    // if slope is 0 or less then there is no worry of overflow happening. "nifi.properties"
    // contains a setting that allows the user to select a threshold value at which time they
    // would like to see graph values begin tracking estimates.
    if (slope <= 0) {
      return alertThreshold;
    }

    // Compute the estimated time to overflow
    double estimatedOverflow = (max - current) / slope;
    estimatedOverflow = BigDecimal.valueOf(estimatedOverflow).setScale(4, RoundingMode.HALF_UP)
        .doubleValue();

    // Return as milliseconds
    return (long) (estimatedOverflow * 60000);
  }

  static long getTimeToByteOverflow() {
    return Math.min(timeToByteOverflow, alertThreshold);
  }

  static long getTimeToCountOverflow() {
    return Math.min(timeToCountOverflow, alertThreshold);
  }

  static long getAlertThreshold() { return alertThreshold; }
}
