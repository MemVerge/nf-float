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


import nextflow.executor.AbstractGridExecutor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskId

import java.nio.file.Paths

class FloatGridExecutorTest extends FloatBaseTest {
    def "get the prefix of kill command"() {
        given:
        def exec = newTestExecutor()
        def id = "myJobId"

        when:
        def out = """
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
        exec.parseJobId(out) == id
    }

    def "kill command"() {
        given:
        def exec = newTestExecutor()
        def jobID = 'myJobID'

        when:
        def cmd = exec.killTaskCommand(jobID)

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'scancel', '-j', jobID].join(' ')
    }

    def "kill commands"() {
        given:
        final exec = newTestExecutor()

        when:
        final commands = exec.killTaskCommands(['a', 'b'])
        def expected = { jobID ->
            return ['float', '-a', addr,
                    '-u', user,
                    '-p', pass,
                    'scancel', '-j', jobID].join(' ')
        }

        then:
        commands[0].join(' ') == expected('a')
        commands[1].join(' ') == expected('b')
    }

    def "submit command line"() {
        given:
        final dataVolume = nfs + ':/data'
        final exec = newTestExecutor([float: [
                address : addr,
                username: user,
                password: pass,
                nfs     : dataVolume]])
        final task = newTask(exec)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(nfs: dataVolume)

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "add default local mount point"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        def expected = submitCmd()

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "use cpus, memory and container"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, new TaskConfig(
                cpus: 8,
                memory: '16 GB',
                container: "biocontainers/star"))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(
                cpu: 8,
                memory: 16,
                image: "biocontainers/star")

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "add common extras"() {
        given:
        final exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         nfs        : nfs,
                         commonExtra: '-t \t small -f ']]
        )
        final task = newTask(exec)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd() + ['-t', 'small', '-f']

        then:
        cmd.join(" ") == expected.join(" ")
    }

    def "add specific extras"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, new TaskConfig(
                extra: ' -f -t \t small  ',
                cpus: 2,
                memory: "4 G",
                container: image))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(
                cpu: 2,
                memory: 4) + ['-f', '-t', 'small']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "incorrect cpu and memory settings"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, new TaskConfig(
                extra: ' -f -t \t small  ',
                cpu: 2,
                mem: 4,
                memory: 2,
                container: image))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(
                cpu: 1,
                memory: 1) + ['-f', '-t', 'small']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "both common extra and specific extra"() {
        given:
        final exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         nfs        : nfs,
                         commonExtra: '-t \t small -f ']]
        )
        final task = newTask(exec, new TaskConfig(
                extra: '--customTag hello',
                cpus: 2,
                memory: "4 G",
                container: image))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(
                cpu: 2,
                memory: 4
        ) + ['-t', 'small', '-f',
             '--customTag', 'hello']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "config level cpu and memory"() {
        given:
        final cpu = 3
        final mem = 6
        final exec = newTestExecutor()
        final task = newTask(exec, new TaskConfig(
                cpus: cpu,
                memory: "$mem G",
                container: image,
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(
                cpu: cpu,
                memory: mem)

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "use default nfs and work dir"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == submitCmd().join(' ')
    }

    def "parse data volume"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, new TaskConfig(
                cpus: cpu,
                memory: "$mem G",
                container: image,
                'extra': '''-M cpu.upperBoundDuration=5s --dataVolume "[size=50]":/BWA_BASE''',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd() + [
                '-M', 'cpu.upperBoundDuration=5s',
                '--dataVolume', '"[size=50]":/BWA_BASE']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    private def taskStatus(int i, String st) {
        return """
            {                                                           
                "id": "task$i",                          
                "name": "tJob-$i",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "$st",
                "customTags": {
                    "nf-job-id": "tJob-$i"
                }
            }
            """
    }

    def "parse queue status"() {
        given:
        def exec = newTestExecutor()
        def taskMap = [
                0 : "Submitted",
                1 : "Initializing",
                2 : "Executing",
                3 : "Floating",
                4 : "Completed",
                5 : "Cancelled",
                6 : "FailToComplete",
                7 : "FailToExecute",
                8 : "Completed",
                9 : "Starting",
                10: "Suspended",
                11: "Suspending",
                12: "Resuming",
                13: "Capturing",
        ]
        def statusList = []
        for (def item : taskMap.entrySet()) {
            statusList.add(taskStatus(item.key, item.value))
        }
        final text = "[" + statusList.join(",") + "]".stripIndent()

        when:
        final count = taskMap.size() - 1
        (0..count).forEach {
            def task = newTask(exec)
            task.workDir = Paths.get(
                    'src', 'test', 'com', 'memverge', 'nextflow')
            task.id = new TaskId(it)
            exec.getHeaderScript(task)
        }


        def dir = Paths.get('src', 'test', 'com', 'memverge')
        exec.floatJobs.setWorkDir(new TaskId(8), dir)
        def res = exec.parseQueueStatus(text)

        then:
        def qs = AbstractGridExecutor.QueueStatus
        res.size() == count + 1
        res['task0'] == qs.PENDING
        res['task1'] == qs.PENDING
        res['task2'] == qs.RUNNING
        res['task3'] == qs.RUNNING
        res['task4'] == qs.DONE
        res['task5'] == qs.ERROR
        res['task6'] == qs.ERROR
        res['task7'] == qs.ERROR
        res['task8'] == qs.UNKNOWN
        res['task9'] == qs.RUNNING
        res['task10'] == qs.RUNNING
        res['task11'] == qs.RUNNING
        res['task12'] == qs.RUNNING
        res['task13'] == qs.RUNNING
    }

    def "retrieve queue status command"() {
        given:
        def exec = newTestExecutor()

        when:
        def cmd = exec.queueStatusCommand(null)

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'squeue', '--format', 'json']
    }

    def "retrieve the credentials from env"() {
        given:
        setEnv('MMC_ADDRESS', addr)
        setEnv('MMC_USERNAME', user)
        setEnv('MMC_PASSWORD', pass)
        final exec = newTestExecutor(
                [float: [cpu: cpu,
                         mem: mem,
                         nfs: nfs]])


        final task = newTask(exec, new TaskConfig(
                cpus: cpu,
                memory: "$mem G",
                container: image))

        when:
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == submitCmd().join(' ')

        cleanup:
        setEnv('MMC_ADDRESS', '')
        setEnv('MMC_USERNAME', '')
        setEnv('MMC_PASSWORD', '')
    }

    def "test to command string"() {
        given:
        final accessKey = 'a-k'
        final secretKey = 's-k'
        final exec = newTestExecutor(
                [podman: [registry: 'quay.io'],
                 aws   : [accessKey: accessKey,
                          secretKey: secretKey],
                 float : [address : addr,
                          username: user,
                          password: pass]])
        final cmd = [
                '-p', pass,
                'key1=' + secretKey,
                'key2=' + accessKey]

        when:
        def str = exec.toLogStr(cmd)

        then:
        str == "-p *** key1=*** key2=***"
    }
}