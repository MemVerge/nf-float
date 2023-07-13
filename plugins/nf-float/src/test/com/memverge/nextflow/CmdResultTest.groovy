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

class CmdResultTest extends Specification {
    def "retrieve job ID from the query output"() {
        given:
        def res = new CmdResult()
        def id = "myJobId"

        when:
        res.out = """
            id: ${id}
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
        """.stripIndent().trim()

        then:
        res.jobID() == id
    }

    def "parse ID when output is empty"() {
        when:
        def res = new CmdResult()

        then:
        res.jobID() == ""
    }

    def "show job detail"() {
        given:
        def res = new CmdResult()

        when:
        res.out = """
            id: NPhpMGikM1HChWRjz2mID                                                                                                                                      
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
            """.stripIndent().trim()

        then:
        res.jobID() == "NPhpMGikM1HChWRjz2mID"
        res.status() == "Completed"
        res.jobRC() == "0"
        !res.isJobRunning()
    }

    def "check is job running"() {
        given:
        def res = new CmdResult()

        when:
        res.out = """
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

        then:
        res.isJobRunning()
    }

    def "get queue status"() {
        given:
        def res = new CmdResult()

        when:
        res.out = """
        [
            {
                "id": "QPj8nsNWfLam6VQWbeGnp",                                                                                           
                "name": "cactus-c5d.large",                                                                                              
                "user": "admin",                                                                                                         
                "imageID": "docker.io/memverge/cactus:latest",                                                                           
                "status": "FailToExecute",
                "customTags": {
                    "nf-job-id": "job-a",
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
                    "nf-job-id": "job-b"
                }
            }
        ]"""
        def stMap = res.getQStatus()
        def st1 = stMap['QPj8nsNWfLam6VQWbeGnp']
        def st2 = stMap['u5x3sSLe0p3OznGavmYu3']

        then:
        st1.status == 'FailToExecute'
        st1.taskID == 'job-a'
        st2.status == 'Executing'
        st2.taskID == 'job-b'
    }

    def "get queue empty"() {
        given:
        def res = new CmdResult()

        when:
        res.out = """No jobs"""
        def stMap = res.getQStatus()

        then:
        stMap.size() == 0
    }
}
