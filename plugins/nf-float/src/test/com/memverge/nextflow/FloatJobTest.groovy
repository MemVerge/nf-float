/*
 * Copyright 2022-2023, MemVerge Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.memverge.nextflow

import spock.lang.Specification

class FloatJobTest extends Specification {
    def "retrieve job ID from the query output"() {
        given:
        final ndJobID = "fd9gd1-23"
        final floatJobID = "float-job-id"
        final out = """
            id: $floatJobID
            name: cactus-
            user: admin
            imageID: docker.io/memverge/cactus:latest
            status: Submitted
            submitTime: "2022-12-08T03:32:32Z"
            duration: 8s
            inputArgs: ' -j /home/zhuanc/cromwell/cromwell-executions/scatterGather/b5fbab4a-8b50-4f5c-9505-01e35e2cb9b0/call-sayHello/shard-2/execution/float-script.sh  -i cactus  -m 4  -c 2 '
            vmPolicy:
                policy: spotOnly
                retryLimit: 3
                retryInterval: 10m0s
            customTags:
                nf-job-id: $ndJobID
        """.stripIndent().trim()

        when:
        final job = FloatJob.parse(out)

        then:
        job.nfJobID == ndJobID
        job.floatJobID == floatJobID
        job.status == FloatStatus.PENDING
        job.rc == ""
        job.rcCode == null
    }

    def "parse ID when output is empty"() {
        when:
        final job = FloatJob.parse("")

        then:
        job.nfJobID == ""
    }

    def "parse ID when not found"() {
        when:
        final job = FloatJob.parse("Error: Invalid argument, invalid job ID (code: 1003)")

        then:
        job.nfJobID == ""
        job.floatJobID == ""
        job.status == FloatStatus.UNKNOWN
    }

    def "show job detail"() {
        given:
        final id = 'myJob-0'
        final floatID = 'NPhpMGikM1HChWRjz2mID'
        final out = """
            id: $floatID                                                                                                                                      
            name: python-c5ad.large         
            user: admin                                                                                                                                            [0/1817]
            imageID: python:3.9-slim
            imageDigest: sha256:7dcd81e0646d2f922516a6bf09e1915d7f7df8438bc4cf6651746bdda27b2a1c
            output: 'container state: exited (0)'
            status: Completed
            submitTime: "2022-12-14T07:07:06Z"
            endTime: "2022-12-14T07:10:12Z"
            lastUpdate: "2022-12-14T07:10:02Z"
            duration: 3m5s
            internalID: 67fb73539c6764c0d12e9be9360c2cbfacd4da796d3ef3913778980ed92f4879
            cost: 0.0017 USD
            rc: "0"
            stdout: stdout.autosave
            stderr: stderr.autosave
            inputArgs: ' -j s3://mmce-test-cicd/mmcetest.sh  -i python  --tag 3.9-slim  --rootVolSize 47  --imageVolSize 4  -m 2  -c 1 '
            customTags:
                nf-job-id: $id
            """.stripIndent().trim()

        when:
        final job = FloatJob.parse(out)

        then:
        job.nfJobID == id
        job.status == FloatStatus.DONE
        job.rc == "0"
        job.rcCode == 0
        !job.isRunning()
    }

    def "check is job running"() {
        given:
        final out = """
            id: QOZCuHxDQlj52mSDBHmJe
            name: cactus-c5ad.large
            workingHost: 35.86.197.248 (2Core4GB/Spot)
            user: admin
            imageID: docker.io/memverge/cactus:latest
            status: Initializing
            submitTime: "2022-12-14T08:43:14Z"
            duration: 2m21s
            cost: 0.0012 USD
            """.stripIndent().trim()

        when:
        final job = FloatJob.parse(out)

        then:
        job.isRunning()
        job.status == FloatStatus.PENDING
    }

    def "check job failed with NoAvailableHost"() {
        given:
        final out = """
            id: uqaj8l6a407u88v0106ol
            name: subread-3nyw14-r6a.2xlarge
            workingHost: ""
            category: failed
            status: NoAvailableHost
            cpu: 8
            duration: 1h5m49s
            queueTime: 0s
            submitTime: "2025-06-20T09:09:34Z"
            execTime: "2025-06-20T09:09:34Z"
            endTime: "2025-06-20T10:15:23Z"
            lastUpdate: "2025-06-20T10:15:23Z"
            memGB: 64
            scheduler: mmcloud
            imageID: quay.io/biocontainers/subread:2.0.6--he4a0461_2
            stdout: stdout.autosave
            stderr: stderr.autosave
            memUsed: 0.00 B
            cpuMax: 24
            memGBMax: 144
            instanceType: r6a.2xlarge
            coreHours: 0.6784628924888889
            """.stripIndent().trim()

        when:
        final job = FloatJob.parse(out)

        then:
        ! job.isRunning()
        job.status == FloatStatus.NOAVAILABLEHOST
    }

    def "check job failed with FailToExecute"() {
        given:
        final out = """
            id: 3cfmkg882qn09q9o8jfkf
            name: rseqc-2609uc-m5zn.3xlarge
            workingHost: ""
            category: failed
            status: FailToExecute
            cpu: 6
            duration: 0s
            queueTime: 0s
            submitTime: "2025-06-20T09:13:54Z"
            execTime: "2025-06-20T09:13:54Z"
            endTime: "2025-06-20T09:13:55Z"
            lastUpdate: "2025-06-20T09:13:55Z"
            memGB: 36
            cpuMax: 24
            memGBMax: 144
            """.stripIndent().trim()

        when:
        final job = FloatJob.parse(out)

        then:
        ! job.isRunning()
        job.status == FloatStatus.ERROR
    }

    def "get queue status"() {
        given:
        final out = """
        [
            {
                "id": "QPj8nsNWfLam6VQWbeGnp",                                                                                           
                "name": "cactus-c5d.large",                                                                                              
                "user": "admin",                                                                                                         
                "imageID": "docker.io/memverge/cactus:latest",                                                                           
                "status": "FailToExecute",
                "rc": "2",
                "customTags": {
                    "nf-job-id": "job-1",
                    "a": "apple"
                }                                                                                                                                                                                                                                                                                                                         
            },
            {
                "id": "u5x3sSLe0p3OznGavmYu3",
                "name": "cactus-t3a.medium",
                "workingHost": "3.143.251.235 (2Core4GB/Spot)",
                "user": "admin",
                "imageID": "docker.io/memverge/cactus:latest",
                "status": "Executing",
                "customTags": {
                    "b": "banana",
                    "nf-job-id": "job-3"
                }
            }
        ]"""

        when:
        def jobs = FloatJob.parseJobMap(out)
        def st1 = jobs.get(0)
        def st2 = jobs.get(1)

        then:
        st1.status == FloatStatus.ERROR
        st1.nfJobID == 'job-1'
        st1.floatJobID == 'QPj8nsNWfLam6VQWbeGnp'
        st1.rcCode == 2
        st2.status == FloatStatus.RUNNING
        st2.nfJobID == 'job-3'
        st2.floatJobID == 'u5x3sSLe0p3OznGavmYu3'
        st2.rcCode == null
    }

    def "get queue empty"() {
        given:
        final out = """No jobs"""

        when:
        def jobs = FloatJob.parseJobMap(out)

        then:
        jobs.size() == 0
    }
}
