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

import org.apache.commons.collections4.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.metrics.EmptyFlowFileEvent;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardEventAccess implements UserAwareEventAccess {
    private final FlowFileEventRepository flowFileEventRepository;
    private final FlowController flowController;

    private static final Logger logger = LoggerFactory.getLogger(StandardEventAccess.class);

    private NiFiProperties properties;

    public StandardEventAccess(final FlowController flowController, final FlowFileEventRepository flowFileEventRepository) {
        this.flowController = flowController;
        this.flowFileEventRepository = flowFileEventRepository;
        properties = new StandardNiFiProperties();
    }

    /**
     * Returns the status of all components in the controller. This request is
     * not in the context of a user so the results will be unfiltered.
     *
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getControllerStatus() {
        return getGroupStatus(flowController.getFlowManager().getRootGroupId());
    }

    /**
     * Returns the status of all components in the specified group. This request
     * is not in the context of a user so the results will be unfiltered.
     *
     * @param groupId group id
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getGroupStatus(final String groupId) {
        final RepositoryStatusReport repoStatusReport = generateRepositoryStatusReport();
        return getGroupStatus(groupId, repoStatusReport);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is not in the context of a user so the
     * results will be unfiltered.
     *
     * @param groupId group id
     * @param statusReport report
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport) {
        final ProcessGroup group = flowController.getFlowManager().getGroup(groupId);

        // this was invoked with no user context so the results will be unfiltered... necessary for aggregating status history
        return getGroupStatus(group, statusReport, authorizable -> true, Integer.MAX_VALUE, 1);
    }


    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents(final long firstEventId, final int maxRecords) throws IOException {
        return new ArrayList<>(getProvenanceRepository().getEvents(firstEventId, maxRecords));
    }

    @Override
    public List<Action> getFlowChanges(final int firstActionId, final int maxActions) {
        final History history = flowController.getAuditService().getActions(firstActionId, maxActions);
        return new ArrayList<>(history.getActions());
    }

    @Override
    public ProvenanceRepository getProvenanceRepository() {
        return flowController.getProvenanceRepository();
    }


    private RepositoryStatusReport generateRepositoryStatusReport() {
        return flowFileEventRepository.reportTransferEvents(System.currentTimeMillis());
    }


    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final NiFiUser user, final int recursiveStatusDepth) {
        final RepositoryStatusReport repoStatusReport = generateRepositoryStatusReport();
        return getGroupStatus(groupId, repoStatusReport, user, recursiveStatusDepth);
    }


    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport, final NiFiUser user) {
        final ProcessGroup group = flowController.getFlowManager().getGroup(groupId);

        // on demand status request for a specific user... require authorization per component and filter results as appropriate
        return getGroupStatus(group, statusReport, authorizable -> authorizable.isAuthorized(flowController.getAuthorizer(), RequestAction.READ, user), Integer.MAX_VALUE, 1);
    }

    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final NiFiUser user) {
        final RepositoryStatusReport repoStatusReport = generateRepositoryStatusReport();
        return getGroupStatus(groupId, repoStatusReport, user);
    }



    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @param recursiveStatusDepth the number of levels deep we should recurse and still include the the processors' statuses, the groups' statuses, etc. in the returned ProcessGroupStatus
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport, final NiFiUser user, final int recursiveStatusDepth) {
        final ProcessGroup group = flowController.getFlowManager().getGroup(groupId);

        // on demand status request for a specific user... require authorization per component and filter results as appropriate
        return getGroupStatus(group, statusReport, authorizable -> authorizable.isAuthorized(flowController.getAuthorizer(), RequestAction.READ, user), recursiveStatusDepth, 1);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. The results will be filtered by executing the specified
     * predicate.
     *
     * @param group group id
     * @param statusReport report
     * @param isAuthorized is authorized check
     * @param recursiveStatusDepth the number of levels deep we should recurse and still include the the processors' statuses, the groups' statuses, etc. in the returned ProcessGroupStatus
     * @param currentDepth the current number of levels deep that we have recursed
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(final ProcessGroup group, final RepositoryStatusReport statusReport, final Predicate<Authorizable> isAuthorized,
                                              final int recursiveStatusDepth, final int currentDepth) {
        if (group == null) {
            return null;
        }

        final ProcessScheduler processScheduler = flowController.getProcessScheduler();

        final ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId(group.getIdentifier());
        status.setName(isAuthorized.evaluate(group) ? group.getName() : group.getIdentifier());
        int activeGroupThreads = 0;
        int terminatedGroupThreads = 0;
        long bytesRead = 0L;
        long bytesWritten = 0L;
        int queuedCount = 0;
        long queuedContentSize = 0L;
        int flowFilesIn = 0;
        long bytesIn = 0L;
        int flowFilesOut = 0;
        long bytesOut = 0L;
        int flowFilesReceived = 0;
        long bytesReceived = 0L;
        int flowFilesSent = 0;
        long bytesSent = 0L;
        int flowFilesTransferred = 0;
        long bytesTransferred = 0;
        long timeToFailureCount = 0L;
        long timeToFailureBytes = 0L;

        final boolean populateChildStatuses = currentDepth <= recursiveStatusDepth;

        // set status for processors
        final Collection<ProcessorStatus> processorStatusCollection = new ArrayList<>();
        status.setProcessorStatus(processorStatusCollection);
        for (final ProcessorNode procNode : group.getProcessors()) {
            final ProcessorStatus procStat = getProcessorStatus(statusReport, procNode, isAuthorized);
            if (populateChildStatuses) {
                processorStatusCollection.add(procStat);
            }
            activeGroupThreads += procStat.getActiveThreadCount();
            terminatedGroupThreads += procStat.getTerminatedThreadCount();
            bytesRead += procStat.getBytesRead();
            bytesWritten += procStat.getBytesWritten();

            flowFilesReceived += procStat.getFlowFilesReceived();
            bytesReceived += procStat.getBytesReceived();
            flowFilesSent += procStat.getFlowFilesSent();
            bytesSent += procStat.getBytesSent();
        }

        // set status for local child groups
        final Collection<ProcessGroupStatus> localChildGroupStatusCollection = new ArrayList<>();
        status.setProcessGroupStatus(localChildGroupStatusCollection);
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final ProcessGroupStatus childGroupStatus;
            if (populateChildStatuses) {
                childGroupStatus = getGroupStatus(childGroup, statusReport, isAuthorized, recursiveStatusDepth, currentDepth + 1);
                localChildGroupStatusCollection.add(childGroupStatus);
            } else {
                // In this case, we don't want to include any of the recursive components' individual statuses. As a result, we can
                // avoid performing any sort of authorizations. Because we only care about the numbers that come back, we can just indicate
                // that the user is not authorized. This allows us to avoid the expense of both performing the authorization and calculating
                // things that we would otherwise need to calculate if the user were in fact authorized.
                childGroupStatus = getGroupStatus(childGroup, statusReport, authorizable -> false, recursiveStatusDepth, currentDepth + 1);
            }

            activeGroupThreads += childGroupStatus.getActiveThreadCount();
            terminatedGroupThreads += childGroupStatus.getTerminatedThreadCount();
            bytesRead += childGroupStatus.getBytesRead();
            bytesWritten += childGroupStatus.getBytesWritten();
            queuedCount += childGroupStatus.getQueuedCount();
            queuedContentSize += childGroupStatus.getQueuedContentSize();

            timeToFailureCount += childGroupStatus.getTimeToFailureCount();
            timeToFailureBytes += childGroupStatus.getTimeToFailureBytes();

            flowFilesReceived += childGroupStatus.getFlowFilesReceived();
            bytesReceived += childGroupStatus.getBytesReceived();
            flowFilesSent += childGroupStatus.getFlowFilesSent();
            bytesSent += childGroupStatus.getBytesSent();

            flowFilesTransferred += childGroupStatus.getFlowFilesTransferred();
            bytesTransferred += childGroupStatus.getBytesTransferred();
        }

        // set status for remote child groups
        final Collection<RemoteProcessGroupStatus> remoteProcessGroupStatusCollection = new ArrayList<>();
        status.setRemoteProcessGroupStatus(remoteProcessGroupStatusCollection);
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            final RemoteProcessGroupStatus remoteStatus = createRemoteGroupStatus(remoteGroup, statusReport, isAuthorized);
            if (remoteStatus != null) {
                if (populateChildStatuses) {
                    remoteProcessGroupStatusCollection.add(remoteStatus);
                }

                flowFilesReceived += remoteStatus.getReceivedCount();
                bytesReceived += remoteStatus.getReceivedContentSize();
                flowFilesSent += remoteStatus.getSentCount();
                bytesSent += remoteStatus.getSentContentSize();
            }
        }

        // connection status
        final Collection<ConnectionStatus> connectionStatusCollection = new ArrayList<>();
        status.setConnectionStatus(connectionStatusCollection);

        // get the connection and remote port status
        for (final Connection conn : group.getConnections()) {
            final boolean isConnectionAuthorized = isAuthorized.evaluate(conn);
            final boolean isSourceAuthorized = isAuthorized.evaluate(conn.getSource());
            final boolean isDestinationAuthorized = isAuthorized.evaluate(conn.getDestination());

            final ConnectionStatus connStatus = new ConnectionStatus();
            connStatus.setId(conn.getIdentifier());
            connStatus.setGroupId(conn.getProcessGroup().getIdentifier());
            connStatus.setSourceId(conn.getSource().getIdentifier());
            connStatus.setSourceName(isSourceAuthorized ? conn.getSource().getName() : conn.getSource().getIdentifier());
            connStatus.setDestinationId(conn.getDestination().getIdentifier());
            connStatus.setDestinationName(isDestinationAuthorized ? conn.getDestination().getName() : conn.getDestination().getIdentifier());
            connStatus.setBackPressureDataSizeThreshold(conn.getFlowFileQueue().getBackPressureDataSizeThreshold());
            connStatus.setBackPressureObjectThreshold(conn.getFlowFileQueue().getBackPressureObjectThreshold());

            final FlowFileEvent connectionStatusReport = statusReport.getReportEntry(conn.getIdentifier());
            if (connectionStatusReport != null) {
                connStatus.setInputBytes(connectionStatusReport.getContentSizeIn());
                connStatus.setInputCount(connectionStatusReport.getFlowFilesIn());
                connStatus.setOutputBytes(connectionStatusReport.getContentSizeOut());
                connStatus.setOutputCount(connectionStatusReport.getFlowFilesOut());

                flowFilesTransferred += connectionStatusReport.getFlowFilesIn() + connectionStatusReport.getFlowFilesOut();
                bytesTransferred += connectionStatusReport.getContentSizeIn() + connectionStatusReport.getContentSizeOut();
            }

            if (isConnectionAuthorized) {
                if (StringUtils.isNotBlank(conn.getName())) {
                    connStatus.setName(conn.getName());
                } else if (conn.getRelationships() != null && !conn.getRelationships().isEmpty()) {
                    final Collection<String> relationships = new ArrayList<>(conn.getRelationships().size());
                    for (final Relationship relationship : conn.getRelationships()) {
                        relationships.add(relationship.getName());
                    }
                    connStatus.setName(StringUtils.join(relationships, ", "));
                }
            } else {
                connStatus.setName(conn.getIdentifier());
            }

            final QueueSize queueSize = conn.getFlowFileQueue().size();
            final int connectionQueuedCount = queueSize.getObjectCount();
            final long connectionQueuedBytes = queueSize.getByteCount();

            // TODO
            QueueOverflowMonitor.computeOverflowEstimate(conn,
                properties.getStatusHistoryThresholdAlert(), 15, flowController);
            timeToFailureBytes = QueueOverflowMonitor.getTimeToByteOverflow();
            timeToFailureCount = QueueOverflowMonitor.getTimeToCountOverflow();
            logger.info(">>>> timeToFailureBytes: " + timeToFailureBytes);
            logger.info(">>>> timeToFailureCount: " + timeToFailureCount);

            connStatus.setTimeToFailureBytes(timeToFailureBytes);
            connStatus.setTimeToFailureCount(timeToFailureCount);

            if (connectionQueuedCount > 0) {
                connStatus.setQueuedBytes(connectionQueuedBytes);
                connStatus.setQueuedCount(connectionQueuedCount);
            }

            if (populateChildStatuses) {
                connectionStatusCollection.add(connStatus);
            }

            queuedCount += connectionQueuedCount;
            queuedContentSize += connectionQueuedBytes;

            final Connectable source = conn.getSource();
            if (ConnectableType.REMOTE_OUTPUT_PORT.equals(source.getConnectableType())) {
                final RemoteGroupPort remoteOutputPort = (RemoteGroupPort) source;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteOutputPort);
            }

            final Connectable destination = conn.getDestination();
            if (ConnectableType.REMOTE_INPUT_PORT.equals(destination.getConnectableType())) {
                final RemoteGroupPort remoteInputPort = (RemoteGroupPort) destination;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteInputPort);
            }
        }

        // status for input ports
        final Collection<PortStatus> inputPortStatusCollection = new ArrayList<>();
        status.setInputPortStatus(inputPortStatusCollection);

        final Set<Port> inputPorts = group.getInputPorts();
        for (final Port port : inputPorts) {
            final boolean isInputPortAuthorized = isAuthorized.evaluate(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isInputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            setRunStatus(port, portStatus);

            // special handling for root group ports
            if (port instanceof RootGroupPort) {
                final RootGroupPort rootGroupPort = (RootGroupPort) port;
                portStatus.setTransmitting(rootGroupPort.isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                flowFilesIn += inputCount;
                bytesIn += inputBytes;
                bytesWritten += entry.getBytesWritten();

                flowFilesReceived += entry.getFlowFilesReceived();
                bytesReceived += entry.getBytesReceived();
            }

            if (populateChildStatuses) {
                inputPortStatusCollection.add(portStatus);
            }

            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        // status for output ports
        final Collection<PortStatus> outputPortStatusCollection = new ArrayList<>();
        status.setOutputPortStatus(outputPortStatusCollection);

        final Set<Port> outputPorts = group.getOutputPorts();
        for (final Port port : outputPorts) {
            final boolean isOutputPortAuthorized = isAuthorized.evaluate(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isOutputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            setRunStatus(port, portStatus);

            // special handling for root group ports
            if (port instanceof RootGroupPort) {
                final RootGroupPort rootGroupPort = (RootGroupPort) port;
                portStatus.setTransmitting(rootGroupPort.isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                bytesRead += entry.getBytesRead();

                flowFilesOut += entry.getFlowFilesOut();
                bytesOut += entry.getContentSizeOut();

                flowFilesSent = entry.getFlowFilesSent();
                bytesSent += entry.getBytesSent();
            }

            if (populateChildStatuses) {
                outputPortStatusCollection.add(portStatus);
            }

            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        for (final Funnel funnel : group.getFunnels()) {
            activeGroupThreads += processScheduler.getActiveThreadCount(funnel);
        }

        status.setActiveThreadCount(activeGroupThreads);
        status.setTerminatedThreadCount(terminatedGroupThreads);
        status.setBytesRead(bytesRead);
        status.setBytesWritten(bytesWritten);
        status.setQueuedCount(queuedCount);
        status.setQueuedContentSize(queuedContentSize);
        status.setInputContentSize(bytesIn);
        status.setInputCount(flowFilesIn);
        status.setOutputContentSize(bytesOut);
        status.setOutputCount(flowFilesOut);
        status.setFlowFilesReceived(flowFilesReceived);
        status.setBytesReceived(bytesReceived);
        status.setFlowFilesSent(flowFilesSent);
        status.setBytesSent(bytesSent);
        status.setFlowFilesTransferred(flowFilesTransferred);
        status.setBytesTransferred(bytesTransferred);
        status.setTimeToFailureBytes(timeToFailureBytes);
        status.setTimeToFailureCount(timeToFailureCount);

        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null && vci.getStatus() != null && vci.getStatus().getState() != null) {
            status.setVersionedFlowState(vci.getStatus().getState());
        }

        return status;
    }

    // pulled out to replace duplicated code
    private void setRunStatus(Port port, PortStatus portStatus) {
        if (ScheduledState.RUNNING.equals(port.getScheduledState())) {
            portStatus.setRunStatus(RunStatus.Running);
        } else if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
            portStatus.setRunStatus(RunStatus.Disabled);
        } else if (!port.isValid()) {
            portStatus.setRunStatus(RunStatus.Invalid);
        } else {
            portStatus.setRunStatus(RunStatus.Stopped);
        }
    }

    // TODO

//    private long [] computeTimeToFailure(final Connection conn) {
//        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//        logger.info(">>>> COMPUTE TTF for Connection : " + conn.getIdentifier());
//        long [] ttf = new long[2];
//
//        int ALERT_THRESHOLD_MINUTES;
//
//        try {
//            if (properties == null) {
//                logger.info(">>>> properties IS NULL");
//            }
//            logger.info(">>>> STATUS_HISTORY_ALERT_THRESHOLD: " + NiFiProperties.STATUS_HISTORY_ALERT_THRESHOLD);
//            ALERT_THRESHOLD_MINUTES = properties.getStatusHistoryThresholdAlert();
//        } catch (Exception ex) {
//            logger.info(">>>> EXCEPTION THROWN getting threshold alert value: " + ex.getMessage());
//            ALERT_THRESHOLD_MINUTES = 240;
//        }
//        logger.info(">>>> ALERT_THRESHOLD_MINUTES: " + ALERT_THRESHOLD_MINUTES);
//
//        // = properties.getStatusHistoryThresholdAlert(); //240;
//
//        // TODO use a user provided value as the default if there is no fear of overflow
//        // Use 12 hours for now
//        ttf[0] = ALERT_THRESHOLD_MINUTES * 60 * 1000;
//        ttf[1] = ALERT_THRESHOLD_MINUTES * 60 * 1000;
//
//        final int HISTORY = -5; // minutes
//        final int HISTORY_OFFSET = Math.abs(HISTORY) + 1;
//        // will need the stats for the connection 15 minutes prior as well as stats for latest
//        // connection.
//        Date currentTime = new Date();
//        Date previousTime = DateUtils.addMinutes(currentTime, HISTORY - 2);
//        logger.info(">>>> prevTime:    " + previousTime.toString());
//        logger.info(">>>> currentTime: " + currentTime.toString());
//        // These dates are used to get the connection history for that time interval.
//        // Shouldn't need more than HISTORY_OFFSET
//
//        List<StatusSnapshotDTO> aggregateSnapshots = flowController
//            .getConnectionStatusHistory(conn.getIdentifier(), previousTime, null, HISTORY_OFFSET)
//            .getAggregateSnapshots();
//
////        StatusHistoryDTO connHistory =
////            flowController.getConnectionStatusHistory(conn.getIdentifier(), previousTime,
////                null, TIME_OFFSET);
////        // get a list of the snapshot data
////        List<StatusSnapshotDTO> aggregateSnapshots = connHistory.getAggregateSnapshots();
//
//        int numberOfSnapshots = aggregateSnapshots.size();
//        logger.info(">>>> aggregateSnapshots.length: " + numberOfSnapshots);
//        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//        for (StatusSnapshotDTO dto : aggregateSnapshots) {
//            logger.info(">>>> Date: " + dto.getTimestamp().toString());
//            Map<String,Long> statusMetrics = dto.getStatusMetrics();
//            logger.info(">>>> metrics: " + statusMetrics.toString());
//        }
//        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//
//        if (numberOfSnapshots < HISTORY_OFFSET) {
//            logger.info(">>>> Not enough snapshots yet (" + numberOfSnapshots + ")...return "
//                + "default values: " + ttf[0]);
//            return ttf;
//        }
//
//        // Grab needed data for all calculations
//        Long countThreshold =
//            Long.valueOf(conn.getFlowFileQueue().getBackPressureObjectThreshold());
//        String backPressureDataSizeThreshold = conn.getFlowFileQueue()
//            .getBackPressureDataSizeThreshold();
//        Long bytesThreshold = convertThresholdToBytes(backPressureDataSizeThreshold);
//
//        logger.info(">>>> countThreshold: " + countThreshold);
//        logger.info(">>>> backPressureDataSizeThreshold: " + backPressureDataSizeThreshold);
//        logger.info(">>>> bytesThreshold: " + bytesThreshold);
//
//        // Would like HISTORY minutes prior to calculating, so if less than HISTORY entries
//        // calculate with what we have until then.
//        int current = numberOfSnapshots - 1;
//        int oldest = Math.max(0, numberOfSnapshots - HISTORY_OFFSET);
//
//        StatusSnapshotDTO oldestSnapshot = aggregateSnapshots.get(oldest);
//        logger.info(">>>> Oldest Date: " + oldestSnapshot.getTimestamp().toString());
//        StatusSnapshotDTO currentSnapshot = aggregateSnapshots.get(current);
//        logger.info(">>>> Current Date: " + currentSnapshot.getTimestamp().toString());
//
//        long currentCount = currentSnapshot.getStatusMetrics().get("queuedCount");
//        long oldestCount = oldestSnapshot.getStatusMetrics().get("queuedCount");
//        logger.info(">>>> currentCount / prevCount: " + currentCount + " / " + oldestCount);
//
//        long currentBytes = currentSnapshot.getStatusMetrics().get("queuedBytes");
//        long oldestBytes = oldestSnapshot.getStatusMetrics().get("queuedBytes");
//        logger.info(">>>> currentBytes / prevBytes: " + currentBytes + " / " + oldestBytes);
//
//        long timeDeltaInMinutes = diffInMinutes(oldestSnapshot.getTimestamp(),
//            currentSnapshot.getTimestamp(), TimeUnit.MINUTES);
//        logger.info(">>>> delta: " + timeDeltaInMinutes);
//
//        if (timeDeltaInMinutes < 1) {
//            logger.info(">>>> time delta still 0...return default values");
//            return ttf;
//        }
//
//        long rawTTFCount = computeTimeToFailureCount(countThreshold, currentCount, oldestCount,
//            timeDeltaInMinutes);
//        long rawTTFBytes = computeTimeToFailureBytes(bytesThreshold, currentBytes, oldestBytes,
//            timeDeltaInMinutes);
//        logger.info(">>>> rawTTFCount: " + rawTTFCount);
//        logger.info(">>>> rawTTFCount: " + rawTTFBytes);
//
//        long ttfCountInMillis = rawTTFCount * 60 * 1000;
//        long ttfBytesInMillis = rawTTFBytes * 60 * 1000;
//
//        logger.info(">>>> ttfCountInMillis: " + ttfCountInMillis);
//        logger.info(">>>> ttfBytesInMillis: " + ttfBytesInMillis);
//
//        if (currentCount == countThreshold) {
//            ttf[0] = 0;
//        } else {
//            ttf[0] = Math.min(ttfCountInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
//        }
//
//        if (currentBytes == bytesThreshold) {
//            ttf[1] = 0;
//        } else {
//            ttf[1] = Math.min(ttfBytesInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
//        }
//
//        //ttf[0] = Math.min(ttfCountInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
//        //ttf[1] = Math.min(ttfBytesInMillis, ALERT_THRESHOLD_MINUTES * 60 * 1000);
//
//        logger.info(">>>> predicted count ttf: " + ttf[0] );
//        logger.info(">>>> predicted byte  ttf: " + ttf[1]);
//
//        return ttf;
//    }
//
//    private long convertThresholdToBytes(String backPressureDataSizeThreshold) {
//        final long BYTES_IN_KILOBYTE = 1024L;
//        final long BYTES_IN_MEGABYTE = 1048576L;
//        final long BYTES_IN_GIGABYTE = 1073741824L;
//        final long BYTES_IN_TERABYTE = 1099511627776L;
//        long bytes;
//
//        String[] threshold = backPressureDataSizeThreshold.split("\\s+");
//        if (threshold[1].toLowerCase().contains("tb")) {
//            bytes = Long.valueOf(threshold[0]) * BYTES_IN_TERABYTE;
//        } else if (threshold[1].toLowerCase().contains("gb")) {
//            bytes = Long.valueOf(threshold[0]) * BYTES_IN_GIGABYTE;
//        } else if (threshold[1].toLowerCase().contains("mb")) {
//            bytes = Long.valueOf(threshold[0]) * BYTES_IN_MEGABYTE;
//        } else if (threshold[1].toLowerCase().contains("kb")) {
//            bytes = Long.valueOf(threshold[0]) * BYTES_IN_KILOBYTE;
//        } else {
//            bytes = Long.valueOf(threshold[0]);
//        }
//        return bytes;
//    }
//
//    private long computeTimeToFailureCount(Long countThreshold, long current, long prev,
//        long delta) {
//        final int ALERT_THRESHOLD_MINUTES = 240;
//        // y = mx + b
//        // y = countThreshold
//        // b = currentCount
//        // m = (current_val - prev_val) / time_delta
//        double slope = (current - prev) / (double)delta;
//        logger.info(">>>> countSlope: " + slope);
//        double dttfCount;
//        if (slope <= 0) {
//          logger.info(">>>> slope is 0 or negative (" + slope + ")...");
//            dttfCount = ALERT_THRESHOLD_MINUTES;
//        } else {
//            dttfCount = (countThreshold - current) / slope;
//        }
//        long ttfCount = (long)dttfCount;
//        return ttfCount;
//    }
//
//    private long computeTimeToFailureBytes(Long bytesThreshold, long current, long prev,
//        long delta) {
//        final int ALERT_THRESHOLD_MINUTES = 240;
//        // y = mx + b
//        // y = countThreshold
//        // b = currentCount
//        // m = (current_val - prev_val) / time_delta
//        double slope = (current - prev) / (double)delta;
//        logger.info(">>>> bytesSlope: " + slope);
//        double dttfBytes;
//        if (slope <= 0) {
//            logger.info(">>>> slope is 0 or negative (" + slope + ")...");
//            dttfBytes = ALERT_THRESHOLD_MINUTES;
//        } else {
//            dttfBytes = (bytesThreshold - current) / slope;
//        }
//        long ttfBytes = (long)dttfBytes;
//        return ttfBytes;
//    }
//
//    private long computeTimeToFailure(Long threshold, long current, long prev, long delta) {
//
//    }
//
//
//    private long diffInMinutes(Date date1, Date date2, TimeUnit timeUnit) {
//        long diffInMillis = date2.getTime() - date1.getTime();
//        return timeUnit.convert(diffInMillis, TimeUnit.MILLISECONDS);
//    }

    private RemoteProcessGroupStatus createRemoteGroupStatus(final RemoteProcessGroup remoteGroup, final RepositoryStatusReport statusReport, final Predicate<Authorizable> isAuthorized) {
        final boolean isRemoteProcessGroupAuthorized = isAuthorized.evaluate(remoteGroup);

        final ProcessScheduler processScheduler = flowController.getProcessScheduler();

        int receivedCount = 0;
        long receivedContentSize = 0L;
        int sentCount = 0;
        long sentContentSize = 0L;
        int activeThreadCount = 0;
        int activePortCount = 0;
        int inactivePortCount = 0;

        final RemoteProcessGroupStatus status = new RemoteProcessGroupStatus();
        status.setGroupId(remoteGroup.getProcessGroup().getIdentifier());
        status.setName(isRemoteProcessGroupAuthorized ? remoteGroup.getName() : remoteGroup.getIdentifier());
        status.setTargetUri(isRemoteProcessGroupAuthorized ? remoteGroup.getTargetUri() : null);

        long lineageMillis = 0L;
        int flowFilesRemoved = 0;
        int flowFilesTransferred = 0;
        for (final Port port : remoteGroup.getInputPorts()) {
            // determine if this input port is connected
            final boolean isConnected = port.hasIncomingConnection();

            // we only want to consider remote ports that we are connected to
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    lineageMillis += portEvent.getAggregateLineageMillis();
                    flowFilesRemoved += portEvent.getFlowFilesRemoved();
                    flowFilesTransferred += portEvent.getFlowFilesOut();
                    sentCount += portEvent.getFlowFilesSent();
                    sentContentSize += portEvent.getBytesSent();
                }
            }
        }

        for (final Port port : remoteGroup.getOutputPorts()) {
            // determine if this output port is connected
            final boolean isConnected = !port.getConnections().isEmpty();

            // we only want to consider remote ports that we are connected from
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    receivedCount += portEvent.getFlowFilesReceived();
                    receivedContentSize += portEvent.getBytesReceived();
                }
            }
        }

        status.setId(remoteGroup.getIdentifier());
        status.setTransmissionStatus(remoteGroup.isTransmitting() ? TransmissionStatus.Transmitting : TransmissionStatus.NotTransmitting);
        status.setActiveThreadCount(activeThreadCount);
        status.setReceivedContentSize(receivedContentSize);
        status.setReceivedCount(receivedCount);
        status.setSentContentSize(sentContentSize);
        status.setSentCount(sentCount);
        status.setActiveRemotePortCount(activePortCount);
        status.setInactiveRemotePortCount(inactivePortCount);

        final int flowFilesOutOrRemoved = flowFilesTransferred + flowFilesRemoved;
        status.setAverageLineageDuration(flowFilesOutOrRemoved == 0 ? 0 : lineageMillis / flowFilesOutOrRemoved, TimeUnit.MILLISECONDS);

        return status;
    }

    private ProcessorStatus getProcessorStatus(final RepositoryStatusReport report, final ProcessorNode procNode, final Predicate<Authorizable> isAuthorized) {
        final boolean isProcessorAuthorized = isAuthorized.evaluate(procNode);

        final ProcessScheduler processScheduler = flowController.getProcessScheduler();

        final ProcessorStatus status = new ProcessorStatus();
        status.setId(procNode.getIdentifier());
        status.setGroupId(procNode.getProcessGroup().getIdentifier());
        status.setName(isProcessorAuthorized ? procNode.getName() : procNode.getIdentifier());
        status.setType(isProcessorAuthorized ? procNode.getComponentType() : "Processor");

        final FlowFileEvent entry = report.getReportEntries().get(procNode.getIdentifier());
        if (entry != null && entry != EmptyFlowFileEvent.INSTANCE) {
            final int processedCount = entry.getFlowFilesOut();
            final long numProcessedBytes = entry.getContentSizeOut();
            status.setOutputBytes(numProcessedBytes);
            status.setOutputCount(processedCount);

            final int inputCount = entry.getFlowFilesIn();
            final long inputBytes = entry.getContentSizeIn();
            status.setInputBytes(inputBytes);
            status.setInputCount(inputCount);

            final long readBytes = entry.getBytesRead();
            status.setBytesRead(readBytes);

            final long writtenBytes = entry.getBytesWritten();
            status.setBytesWritten(writtenBytes);

            status.setProcessingNanos(entry.getProcessingNanoseconds());
            status.setInvocations(entry.getInvocations());

            status.setAverageLineageDuration(entry.getAverageLineageMillis());

            status.setFlowFilesReceived(entry.getFlowFilesReceived());
            status.setBytesReceived(entry.getBytesReceived());
            status.setFlowFilesSent(entry.getFlowFilesSent());
            status.setBytesSent(entry.getBytesSent());
            status.setFlowFilesRemoved(entry.getFlowFilesRemoved());

            if (isProcessorAuthorized) {
                status.setCounters(entry.getCounters());
            }
        }

        // Determine the run status and get any validation error... only validating while STOPPED
        // is a trade-off we are willing to make, even though processor validity could change due to
        // environmental conditions (property configured with a file path and the file being externally
        // removed). This saves on validation costs that would be unnecessary most of the time.
        if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
            status.setRunStatus(RunStatus.Disabled);
        } else if (ScheduledState.RUNNING.equals(procNode.getScheduledState())) {
            status.setRunStatus(RunStatus.Running);
        } else if (procNode.getValidationStatus() == ValidationStatus.VALIDATING) {
            status.setRunStatus(RunStatus.Validating);
        } else if (procNode.getValidationStatus() == ValidationStatus.INVALID) {
            status.setRunStatus(RunStatus.Invalid);
        } else {
            status.setRunStatus(RunStatus.Stopped);
        }

        status.setExecutionNode(procNode.getExecutionNode());
        status.setTerminatedThreadCount(procNode.getTerminatedThreadCount());
        status.setActiveThreadCount(processScheduler.getActiveThreadCount(procNode));

        return status;
    }
}
