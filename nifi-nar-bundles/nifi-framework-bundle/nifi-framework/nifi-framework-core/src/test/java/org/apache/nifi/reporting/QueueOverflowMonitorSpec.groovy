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

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

@Unroll
class QueueOverflowMonitorSpec extends Specification {

    // given
    // when
    // then
    // where

    def "Test convert date difference to minutes"() {
        given:
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

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

    // y = mx + b
    // m = slope --> rise/run --> (current_val - prev_val) / time_delta (in minutes)
    // y = overflow limit
    // b = current value of bytes/count
    // solve for x
    def "Compute overflow estimate"() {
        when:
        def estimate = QueueOverflowMonitor.getTimeToOverflow(max, current, prev, delta)

        then:
        double slope = (current - prev) / delta
        long lslope = (long) slope
        long expected = (max  - current) / slope
        println("estimate: " + estimate)
        println("expected: " + expected)
        estimate == expected

        where:
        max | current | prev | delta
        1000 | 50 | 10 | 15
        1000 | 1  | 1  | 15
        1000 | 99 | 98 | 15
        1000 | 538 | 536 | 1
        1000 | 541 | 536 | 2

    }
}

