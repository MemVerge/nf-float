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
            customTags:
                input-size: "123456789"
                job-kind: file-NFCORE
                nextflow-io-process-name: NFCORE-PROCESS-NAME
                nextflow-io-project-name: project-name
                nextflow-io-run-name: agitated-nightingale
                nextflow-io-session-id: uuid-a-b-c-d-e
                nextflow-io-task-name: NFCORE-a-b-c-sorted-bam
                nf-job-id: tJob-60
        """.stripIndent().trim()

        then:
        exec.parseJobId(out) == id
        def job = exec.getJob(new TaskId(60))
        job.status == FloatStatus.PENDING
    }

    def "parse job id failed"() {
        given:
        def exec = newTestExecutor()

        when:
        final out = "Error: open abc.sh: no such file or directory"

        then:
        exec.parseJobId(out) == ""
    }

    def "kill command"() {
        given:
        def exec = newTestExecutor()
        def jobID = 'myJobID'

        when:
        //noinspection GroovyAccessibility
        def cmd = exec.killTaskCommand(jobID)

        then:
        cmd.join(' ') == [bin, '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'cancel', '-j', jobID].join(' ')
    }

    def "kill commands"() {
        given:
        final exec = newTestExecutor()
        exec.floatJobs.updateJob(FloatJob.parse(
                """
                  id: a
                  customTags:
                      nf-job-id: tJob-1
                  """))
        exec.floatJobs.updateJob(FloatJob.parse(
                """
                  id: b
                  customTags:
                      nf-job-id: tJob-2
                  """))

        when:
        final commands = exec.killTaskCommands(['a', 'b'])
        def expected = { jobID ->
            return [bin, '-a', addr,
                    '-u', user,
                    '-p', pass,
                    'cancel', '-j', jobID, '-f'].join(' ')
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
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(nfs: dataVolume)

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "max cpu and memory factor"() {
        given:
        final dataVolume = nfs + ':/data'
        final exec = newTestExecutor([float: [
                address        : addr,
                maxCpuFactor   : 3,
                maxMemoryFactor: 2.51,
                nfs            : dataVolume]])
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(nfs: dataVolume)

        then:
        cmd.join(' ').contains("--cpu 5:15 --mem 10:25")
    }

    def "add default local mount point"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        def expected = submitCmd()

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "use cpus, memory and container"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
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
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd() + ['-t', 'small', '-f']

        then:
        cmd.join(" ") == expected.join(" ")
    }

    def "quoted arguments in extra"() {
        given:
        final exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         nfs        : nfs,
                         commonExtra: '--dataVolume  [opts="-o allow_other"]s3://1.2.3.4:/a:/a  --rootVolSize 10']]
        )
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd() + [
                '--dataVolume',
                '[opts="-o allow_other"]s3://1.2.3.4:/a:/a',
                '--rootVolSize',
                '10']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "add specific extras"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
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
        final task = newTask(exec, 0, new TaskConfig(
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
        final task = newTask(exec, 0, new TaskConfig(
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

    def "define both common extra and ext float"() {
        given:
        final exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         nfs        : nfs,
                         commonExtra: '-f']]
        )
        final task = newTask(exec, 0, new TaskConfig(
                ext: [float: '-t small'],
                cpus: 2,
                memory: "4 G",
                container: image))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd(cpu: 2, memory: 4) + ['-f', '-t', 'small']

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "config level cpu and memory"() {
        given:
        final cpu = 3
        final mem = 6
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
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
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == submitCmd().join(' ')
    }

    def "trim space and quotes in image"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                cpus: cpu,
                memory: "$mem G",
                container: "\"' $image\t'\"",
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))
        final expected = submitCmd()

        then:
        cmd.join(' ') == expected.join(' ')
    }

    def "parse data volume"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
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

    def "use machine type directive"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                machineType: 't2.xlarge',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--instType t2.xlarge')
    }

    def "use disk directive"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                disk: '41G',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--imageVolSize 41')
    }

    def "by default, ignore the time directive"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                time: '24h',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        !cmd.join(' ').contains('--timeLimit')
    }

    def "use time directive"() {
        given:
        final exec = newTestExecutor([
                float: [address         : addr,
                        username        : user,
                        password        : pass,
                        nfs             : nfs,
                        ignoreTimeFactor: false]
        ])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                time: '24h',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--timeLimit 86400s')
    }

    def "use timeout factor"() {
        given:
        final exec = newTestExecutor(
                [float: [address         : addr,
                         username        : user,
                         password        : pass,
                         nfs             : nfs,
                         ignoreTimeFactor: false,
                         timeFactor      : 1.1]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                time: '1h',
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--timeLimit 3960s')
    }

    def "use on-demand for retry"() {
        given:
        final exec = newTestExecutor(
                [float: [address : addr,
                         username: user,
                         password: pass,
                         nfs     : nfs]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                time: '1h',
                attempt: 2,
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('onDemand=true')
    }

    def "use cpu factor"() {
        given:
        final exec = newTestExecutor(
                [float: [address  : addr,
                         username : user,
                         password : pass,
                         nfs      : nfs,
                         cpuFactor: 1.5]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                cpus: 2,
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--cpu 3')
    }

    def "use invalid cpu"() {
        given:
        final exec = newTestExecutor(
                [float: [address  : addr,
                         username : user,
                         password : pass,
                         nfs      : nfs,
                         cpuFactor: 0.2]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                cpus: 1,
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--cpu 1')
    }

    def "use memory factor"() {
        given:
        final exec = newTestExecutor(
                [float: [address     : addr,
                         username    : user,
                         password    : pass,
                         nfs         : nfs,
                         memoryFactor: 0.5]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                memory: "8 GB",
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--mem 4')
    }

    def "use invalid memory"() {
        given:
        final exec = newTestExecutor(
                [float: [address     : addr,
                         username    : user,
                         password    : pass,
                         nfs         : nfs,
                         memoryFactor: 0.5]])
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                memory: 8,
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--mem 1')
    }

    def "use extra options"() {
        given:
        final option = """--external 'mnt[]:sm'"""
        final exec = newTestExecutor([
                float: [address     : addr,
                        username    : user,
                        password    : pass,
                        nfs         : nfs,
                        extraOptions: option]])
        final task = newTask(exec, 0)

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.contains('--extraOptions')
        cmd.contains(option)
    }

    def "use resourceLabels directive"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                container: image,
                resourceLabels: [foo: 'bar'],
        ))

        when:
        final cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ').contains('--customTag foo:bar')
    }

    private static def taskStatus(int i, String st) {
        return """
                id: task$i,
                name: tJob-$i
                user: admin
                imageID: docker.io/memverge/cactus:latest,
                status: $st,
                customTags:
                    nf-job-id: tJob-$i
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

        when:
        final count = taskMap.size()
        // update a list of the task status
        for (def entry : taskMap.entrySet()) {
            exec.floatJobs.updateJob(FloatJob.parse(taskStatus(entry.key, entry.value)))
        }
        def res = exec.parseQueueStatus("")

        then:
        //noinspection GroovyAccessibility
        def qs = AbstractGridExecutor.QueueStatus
        res.size() == count
        res['tJob-0'] == qs.PENDING
        res['tJob-1'] == qs.PENDING
        res['tJob-2'] == qs.RUNNING
        res['tJob-3'] == qs.RUNNING
        res['tJob-4'] == qs.DONE
        res['tJob-5'] == qs.ERROR
        res['tJob-6'] == qs.ERROR
        res['tJob-7'] == qs.ERROR
        res['tJob-8'] == qs.DONE
        res['tJob-9'] == qs.RUNNING
        res['tJob-10'] == qs.RUNNING
        res['tJob-11'] == qs.RUNNING
        res['tJob-12'] == qs.RUNNING
        res['tJob-13'] == qs.RUNNING
    }

    def "retrieve queue status command"() {
        given:
        def exec = newTestExecutor()

        when:
        def cmd = exec.queueStatusCommand(null)

        then:
        cmd == []
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


        final task = newTask(exec, 0, new TaskConfig(
                cpus: cpu,
                memory: "$mem G",
                container: image))

        when:
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == submitCmd().join(' ')

        cleanup:
        setEnv('MMC_ADDRESS')
        setEnv('MMC_USERNAME')
        setEnv('MMC_PASSWORD')
    }

    def "get disk size when scratch is not enabled"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0)

        when:
        List<String> cmd = []
        exec.addVolSize(cmd, task)

        then:
        cmd.join(' ') == ""
    }

    def "get disk size when specified in task"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(disk: '18.GB'))

        when:
        List<String> cmd = []
        exec.addVolSize(cmd, task)

        then:
        cmd.join(' ') == "--imageVolSize 18"
    }

    def "get disk size when specified size is smaller than min"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(disk: '2.GB'))

        when:
        List<String> cmd = []
        exec.addVolSize(cmd, task)

        then:
        // do not specify size because it's smaller than min
        cmd.join(' ') == ""
    }

    def "default scratch to true, stage in mode to copy"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0)

        when:
        def builder = exec.createBashWrapperBuilder(task)

        then:
        builder.scratch == true
        builder.stageInMode == 'copy'
    }

    def "use custom scratch and stage in mode"() {
        given:
        final exec = newTestExecutor()
        final task = newTask(exec, 0, new TaskConfig(
                scratch: false,
                stageInMode: 'link'))

        when:
        def builder = exec.createBashWrapperBuilder(task)

        then:
        builder.scratch == false
        builder.stageInMode == 'link'
    }
}
