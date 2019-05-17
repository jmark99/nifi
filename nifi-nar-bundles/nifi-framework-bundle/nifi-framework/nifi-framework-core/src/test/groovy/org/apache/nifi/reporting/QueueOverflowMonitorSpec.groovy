package org.apache.nifi.reporting

import org.apache.nifi.connectable.Connection
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.util.NiFiProperties;
import spock.lang.Specification;

//import static org.mockito.Mockito.mock;


class QueueOverflowMonitorSpec extends Specification {

    NiFiProperties properties

    def "retrieving the ttf for counts"() {
        given: "a mock Connection object"
        Connection connection = Mock Connection

        and: "a call to computeOverflowEstimate"
        QueueOverflowMonitor.computeOverflowEstimate(connection, 100);

        expect:
        QueueOverflowMonitor.getTimeToCountOverflow() == 5L
    }

    def "retrieving the ttf for bytes"() {
        given: "a mock Connection object"
        Connection connection = Mock Connection

        and: "a call to computeOverflowEstimate"
        QueueOverflowMonitor.computeOverflowEstimate(connection, 110);

        expect:
        QueueOverflowMonitor.getTimeToByteOverflow() == 3L
    }

    def "retrieve alert threshold"() {
        given: "a mock Connection"
        Connection connection = Mock Connection

        and: "a call to compute the overflow estimate"
        QueueOverflowMonitor.computeOverflowEstimate(connection, 150)

        expect: "the default value of 2 hours to be returned"
        QueueOverflowMonitor.getAlertThresholdInMinutes() == 150
    }

    def "retrieve alert threshold using properties"() {
        given: "a mock Connection"
        Connection connection = Mock Connection

        and: "a call to compute the overflow estimate"
        QueueOverflowMonitor.computeOverflowEstimate(connection, properties.getStatusHistoryThresholdAlert())

        expect: "the default value of 2 hours to be returned"
        QueueOverflowMonitor.getAlertThresholdInMinutes() == 120
    }
}