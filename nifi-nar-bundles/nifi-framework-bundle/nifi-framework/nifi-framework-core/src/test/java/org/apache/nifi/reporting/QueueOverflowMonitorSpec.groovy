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
package org.apache.nifi.reporting

import org.apache.commons.lang3.time.DateUtils
import spock.lang.Specification
import spock.lang.Unroll

import java.math.RoundingMode
import java.util.concurrent.TimeUnit

@Unroll
class QueueOverflowMonitorSpec extends Specification {

    def "Test convert date difference to minutes"() {
        when:
        long diffInMinutes = QueueOverflowMonitor.diffInMinutes(date1, date2)

        then:
        long millis = Math.abs(date2.getTime() - date1.getTime())
        diffInMinutes == TimeUnit.MINUTES.convert(millis, TimeUnit.MILLISECONDS)

        where:
        date1 | date2
        DateUtils.parseDate("2019/08/12 23:22:10", "yyyy/MM/dd HH:mm:ss") |
                DateUtils.parseDate("2019/08/13 23:22:10", "yyyy/MM/dd HH:mm:ss")
        DateUtils.parseDate("2019/08/12 10:11:12", "yyyy/MM/dd HH:mm:ss") |
                DateUtils.parseDate("2019/08/12 10:11:12", "yyyy/MM/dd HH:mm:ss")
        DateUtils.parseDate("2019/08/12 00:00:00", "yyyy/MM/dd HH:mm:ss") |
                DateUtils.parseDate("2019/08/12 00:15:00", "yyyy/MM/dd HH:mm:ss")
        DateUtils.parseDate("2019/08/12 00:00:00", "yyyy/MM/dd HH:mm:ss") |
                DateUtils.parseDate("2019/08/12 00:15:31", "yyyy/MM/dd HH:mm:ss")
        DateUtils.parseDate("2019/08/12 00:00:00", "yyyy/MM/dd HH:mm:ss") |
                DateUtils.parseDate("2019/08/22 13:15:34", "yyyy/MM/dd HH:mm:ss")
    }

    def "Compute overflow estimate"() {
        given:
        double estimatedOverflow
        double slope = (current - prev) / (double) delta
        if (slope <= 0) {
            estimatedOverflow = QueueOverflowMonitor.getAlertThreshold()
        } else {
            slope = BigDecimal.valueOf(slope).setScale(2, RoundingMode.HALF_UP).doubleValue()
            estimatedOverflow = (max - current) / slope
            estimatedOverflow = BigDecimal.valueOf(estimatedOverflow).setScale(4, RoundingMode
                    .HALF_UP).doubleValue()
            estimatedOverflow = (long) (estimatedOverflow * 60000)
        }

        when:
        def expected = QueueOverflowMonitor.getTimeToOverflow(max, current, prev, delta)

        then:
        expected == estimatedOverflow

        where:
        max | current | prev | delta
        1000 | 50 | 10 | 15
        1000 | 99 | 98 | 15
        1000 | 538 | 536 | 1
        1000 | 541 | 536 | 2
        1000 | 1  | 1  | 15
        1000 | 0  | 0  | 12
        1000 | 27  | 24  | 15
        1000 | 24  | 27  | 15
        1000 | 10  | 10  | 15
        1000 | 2000  | 2000  | 15
    }
}

