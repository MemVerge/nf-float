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

import nextflow.Session
import nextflow.processor.TaskConfig
import nextflow.processor.TaskId
import nextflow.processor.TaskRun
import spock.lang.Specification

import java.nio.file.Paths

class FloatGridExecutorMultiOCTest extends Specification {
    def addr = 'fa, fb,'
    def user = 'admin'
    def pass = 'password'
    def nfs = 'nfs://a.b.c'
    def cpu = 5
    def mem = 10
    def taskID = 55
    def image = 'cactus'
    def script = '/path/job.sh'
    def tJob = 'tJob'

    def newTestExecutor(Map config = null) {
        if (config == null) {
            config = [float: [address : addr,
                              username: user,
                              password: pass]]
        }
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session
        sess.config = config
        exec.session = sess
        exec.floatJobs.setTaskPrefix(tJob)
        return exec
    }

    def "submit job with round robin"() {
        given:
        def exec = newTestExecutor()
        def dataVol = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs  : dataVol,
                      cpu  : cpu,
                      mem  : mem,
                      image: image] as TaskConfig

        when:
        task.id = new TaskId(taskID)
        task.config = config
        def cmd1 = exec.getSubmitCommandLine(task, Paths.get(script))
        def cmd2 = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd1.join(' ') == "float -a fb -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}" +
                " --name tJob-${taskID}"
        cmd2.join(' ') == "float -a fa -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}" +
                " --name tJob-${taskID}"
    }

    def "get queue status commands"() {
        given:
        def exec = newTestExecutor()

        when:
        def cmdMap = exec.queueStatusCommands()
        def cmd1 = cmdMap['fa']
        def cmd2 = cmdMap['fb']

        then:
        cmd1.join(' ') == "float -a fa -u ${user} -p ${pass} " +
                "squeue --format json"
        cmd2.join(' ') == "float -a fb -u ${user} -p ${pass} " +
                "squeue --format json"
    }

    def "input multiple addresses as list"() {
        given:
        def exec = newTestExecutor(
                [float: [address : ['fa', 'fb'],
                         username: user,
                         password: pass]])

        def dataVol = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs  : dataVol,
                      cpu  : cpu,
                      mem  : mem,
                      image: image] as TaskConfig

        when:
        task.id = new TaskId(taskID)
        task.config = config
        def cmd1 = exec.getSubmitCommandLine(task, Paths.get(script))
        def cmd2 = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd1.join(' ') == "float -a fb -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}" +
                " --name tJob-${taskID}"
        cmd2.join(' ') == "float -a fa -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}" +
                " --name tJob-${taskID}"
    }
}
