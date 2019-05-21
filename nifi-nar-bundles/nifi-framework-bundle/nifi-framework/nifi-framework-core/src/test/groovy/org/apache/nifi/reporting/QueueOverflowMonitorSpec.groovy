package org.apache.nifi.reporting

import org.apache.nifi.connectable.Connection
import org.apache.nifi.controller.FlowController
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import spock.lang.Specification;

//import static org.mockito.Mockito.mock;


class QueueOverflowMonitorSpec extends Specification {

    NiFiProperties properties
    int window = 5
    FlowController flowController
    Connection connection
    Date date
    StatusHistoryDTO statusHistoryDTO

//    List<StatusSnapshotDTO> snapshots = flowController
//            .getConnectionStatusHistory(conn.getIdentifier(), startTime, null, offset)
//            .getAggregateSnapshots();

    def setup() {
        flowController = Mock()
        connection = Mock()
        statusHistoryDTO = Mock()

        date = new Date()

        List<StatusSnapshotDTO> snapshots = new ArrayList<>()
        StatusSnapshotDTO snapshotDTO = new StatusSnapshotDTO()

        statusHistoryDTO.setAggregateSnapshots(new ArrayList<StatusSnapshotDTO>(snapshots))

        snapshotDTO.timestamp = new Date()
        snapshotDTO.statusMetrics = new HashMap<String, Long>()
        snapshotDTO.statusMetrics.put("count", 40)
        snapshots << snapshotDTO

        connection.getIdentifier() >>
                "connId"

        flowController.getConnectionStatusHistory(*_) >>
                statusHistoryDTO

        statusHistoryDTO.aggregateSnapshots() >>
                snapshots

//        flowController.getConnectionStatusHistory(connection.getIdentifier(), date, date, 5)
//                .aggregateSnapshots() >>
//                snapshots

        properties = new StandardNiFiProperties()


    }

    def "get snapshots"() {
        when: "call aggregateSnapshots"
        List<StatusSnapshotDTO> history = flowController.getConnectionStatusHistory(connection.getIdentifier(),
                date, date, 5).aggregateSnapshots()

        then: "return a list of snapshots"
        println("snapshots: " + snapshots().toString())

    }

//    def "retrieving the ttf for counts"() {
//        given: "a mock Connection object"
//        Connection connection = Mock Connection
//
//        and: "a call to computeOverflowEstimate"
//        QueueOverflowMonitor.computeOverflowEstimate("id", 100, window, flowController)
//
//        expect:
//        QueueOverflowMonitor.getTimeToCountOverflow() == 5L
//    }

//    def "retrieving the ttf for bytes"() {
//        given: "a mock Connection object"
//        Connection connection = Mock Connection
//
//        and: "a call to computeOverflowEstimate"
//        QueueOverflowMonitor.computeOverflowEstimate("id", 110, window, flowController)
//
//        expect:
//        QueueOverflowMonitor.getTimeToByteOverflow() == 3L
//    }

//    def "retrieve alert threshold"() {
//        given: "a mock Connection"
//        Connection connection = Mock Connection
//
//        and: "a call to compute the overflow estimate"
//        QueueOverflowMonitor.computeOverflowEstimate(connection, 150)
//
//        expect: "the default value of 2 hours to be returned"
//        QueueOverflowMonitor.getAlertThresholdInMinutes() == 150
//    }

//    def "retrieve alert threshold using properties"() {
//        given: "a mock Connection"
//        Connection connection = Mock Connection
//
//        and: "a call to compute the overflow estimate"
//        QueueOverflowMonitor.computeOverflowEstimate(connection, properties.getStatusHistoryThresholdAlert())
//
//        expect: "the default value of 2 hours to be returned"
//        QueueOverflowMonitor.getAlertThresholdInMinutes() == 120
//    }
}