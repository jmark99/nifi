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
package org.apache.nifi.cluster.manager

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.PortStatusDTO
import org.apache.nifi.web.api.dto.status.PortStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class PermissionBasedStatusMergerSpec extends Specification {
    def "Merge ConnectionStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                                                 | targetCanRead |
                toMerge                                                                                                                                                  | toMergeCanRead ||
                expectedDto
        new ConnectionStatusDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real')               | true          |
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')                                                                                                                       | false          ||
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')
        new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden') | false         |
                new ConnectionStatusDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') | true           ||
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden')
    }

    def "Merge ConnectionStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target | targetCanRead | toMerge | toMergeCanRead || expectedDto
        // case 1
        new ConnectionStatusSnapshotDTO(
                groupId: 'real',
                id: 'real',
                name: 'real',
                sourceId: 'real',
                sourceName: 'real',
                destinationId: 'real',
                destinationName: 'real') | true |
        new ConnectionStatusSnapshotDTO(
                groupId: 'hidden',
                id: 'hidden',
                name: 'hidden',
                sourceId: 'hidden',
                sourceName: 'hidden',
                destinationId: 'hidden',
                destinationName: 'hidden') |false ||
        new ConnectionStatusSnapshotDTO(
                groupId: 'hidden',
                id: 'hidden',
                name: 'hidden',
                sourceId: 'hidden',
                sourceName: 'hidden',
                destinationId: 'hidden',
                destinationName: 'hidden',
                timeToOverflow: '00:00:00 / 00:00:00',
                input: '0 (0 bytes)',
                output: '0 (0 bytes)',
                queued: '0 (0 bytes)',
                queuedSize: '0 bytes',
                queuedCount: '0')
        // case 2
        new ConnectionStatusSnapshotDTO(
                groupId: 'hidden',
                id: 'hidden',
                name: 'hidden',
                sourceId: 'hidden',
                sourceName: 'hidden',
                destinationId: 'hidden',
                destinationName: 'hidden') | false |
        new ConnectionStatusSnapshotDTO(
                groupId: 'real',
                id: 'real',
                name: 'real',
                sourceId: 'real',
                sourceName: 'real',
                destinationId: 'real',
                destinationName: 'real') | true ||
        new ConnectionStatusSnapshotDTO(
                groupId: 'hidden',
                id: 'hidden',
                name: 'hidden',
                sourceId: 'hidden',
                sourceName: 'hidden',
                destinationId: 'hidden',
                destinationName: 'hidden',
                timeToOverflow: '00:00:00 / 00:00:00',
                input: '0 (0 bytes)',
                output: '0 (0 bytes)',
                queued: '0 (0 bytes)',
                queuedSize: '0 bytes',
                queuedCount: '0')
    }

    def "Merge PortStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                             | targetCanRead |
                toMerge                                                            | toMergeCanRead ||
                expectedDto
        new PortStatusDTO(groupId: 'real', id: 'real', name: 'real', transmitting: 'false')       | true          |
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false') | false          ||
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false')
        new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false') | false         |
                new PortStatusDTO(groupId: 'real', id: 'real', name: 'real', transmitting: 'false')       | true           ||
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false')
    }

    def "Merge PortStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                     | targetCanRead |
                toMerge                                                                    | toMergeCanRead ||
                expectedDto
        new PortStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real')       | true          |
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden') | false          ||
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
        new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden') | false         |
                new PortStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real')       | true           ||
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
    }

    def "Merge ProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                  | targetCanRead |
                toMerge                                                                                                                   | toMergeCanRead ||
                expectedDto
        new ProcessGroupStatusDTO(id: 'real', name: 'real')     | true          | new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden') | false          ||
                new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden')
        new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden') | false         | new ProcessGroupStatusDTO(id: 'real', name: 'real')     | true           ||
                new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden')
    }

    def "Merge ProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                          | targetCanRead |
                toMerge                                                                                                                                   | toMergeCanRead ||
                expectedDto
        new ProcessGroupStatusSnapshotDTO(id: 'real', name: 'real')     | true          | new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden') | false          ||
                new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0' +
                        ' bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
        new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden') | false         | new ProcessGroupStatusSnapshotDTO(id: 'real', name: 'real')     | true           ||
                new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0 bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
    }

    def "Merge ProcessorStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                  | targetCanRead |
                toMerge                                                                                 | toMergeCanRead ||
                expectedDto
        new ProcessorStatusDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true          |
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false          ||
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
        new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false         |
                new ProcessorStatusDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true           ||
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
    }

    def "Merge ProcessorStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                          | targetCanRead |
                toMerge                                                                                         | toMergeCanRead ||
                expectedDto
        new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false         |
                new ProcessorStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true           ||
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
        new ProcessorStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true          |
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false          ||
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
    }

    def "Merge RemoteProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                | targetCanRead |
                toMerge                                                                                               | toMergeCanRead ||
                expectedDto
        new RemoteProcessGroupStatusDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true          |
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false          ||
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
        new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false         |
                new RemoteProcessGroupStatusDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true           ||
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
    }

    def "Merge RemoteProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                        | targetCanRead |
                toMerge                                                                                                       | toMergeCanRead ||
                expectedDto
        new RemoteProcessGroupStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true          |
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false          ||
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
        new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false         |
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true           ||
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
    }


    def "Verify Summary Page Time-to-Overflow Converts Correctly"() {
        when:
        def prettyString = StatusMerger.prettyPrintTTF(msCount, msBytes)

        then:
        prettyString == ms2time(msCount) + " / " + ms2time(msBytes)

        where:
        msCount  | msBytes
        0        | 0
        12345678 | 78445978
        94879318 | 524252
         7493628 | 476342
        86400000 | 86400000
        86200000 | 86399999
       200000000 | 200000000
    }

    // utility method used in testing Time-to-Overflow values
    def "ms2time"(long ms) {
        int ONE_DAY = 86400000
        String GREATER_THAN_DAY = "> 24:00:00"

        if (ms >= ONE_DAY) {
            return GREATER_THAN_DAY
        }
        int millis = ms
        int seconds = (millis / 1000).remainder(60)
        seconds = (int)seconds
        int minutes = (millis / (1000 * 60)).remainder(60)
        minutes = (int)minutes
        int hours = (millis / (1000 * 60 * 60))
        hours = (int)hours
        return sprintf("%02d:%02d:%02d", hours, minutes, seconds)
    }
}
