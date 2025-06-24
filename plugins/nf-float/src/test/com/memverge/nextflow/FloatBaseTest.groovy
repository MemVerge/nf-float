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
import nextflow.processor.*
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

class BaseTest extends Specification {
    def setEnv(String key, String value = "") {
        Global.setEnv(key, value)
    }
}

class FloatBaseTest extends BaseTest {
    def addr = 'float.my.com'
    def user = 'admin'
    def pass = 'password'
    def nfs = 'nfs://a.b.c'
    def tJob = 'tJob'
    def cpu = 5
    def mem = 10
    def image = 'cactus'
    def script = '/path/job.sh'
    def workDir = '/mnt/nfs/shared'
    def uuid = UUID.fromString("00000000-0000-0000-0000-000000000000")
    def bin = FloatBin.get("").toString()
    private AtomicInteger taskSerial = new AtomicInteger()

    class FloatTestExecutor extends FloatGridExecutor {
        @Override
        String downloadScriptFile(Path scriptFile) {
            // do nothing in the test
            return "/tmp/"
        }
    }

    def newTestExecutor(Map config = null) {
        if (config == null) {
            config = [float  : [address : addr,
                                username: user,
                                password: pass,
                                nfs     : nfs],
                      process: [executor: 'float']]
        }
        def exec = new FloatTestExecutor()
        def sess = [:] as Session
        sess.config = config
        exec.session = sess
        exec.session.workDir = Paths.get(workDir)
        exec.floatJobs.setTaskPrefix(tJob)
        exec.session.runName = 'test-run'
        //noinspection GroovyAccessibility
        exec.session.uniqueId = uuid
        return exec
    }

    def newTask(FloatTestExecutor exec, int id, TaskConfig conf = null) {
        if (conf == null) {
            conf = new TaskConfig(cpus: cpu,
                    memory: "$mem G",
                    container: image)
        }
        def task = new TaskRun()
        task.processor = Mock(TaskProcessor)
        task.processor.getSession() >> Mock(Session)
        task.processor.getExecutor() >> exec
        task.processor.getProcessEnvironment() >> [:]
        task.config = conf
        task.id = new TaskId(id)
        task.index = taskSerial.incrementAndGet()
        task.workDir = Paths.get(workDir)
        task.name = "foo (${task.id})"
        return task
    }

    def newTaskBean(FloatTestExecutor exec, int id, TaskConfig conf = null) {
        def task = newTask(exec, id, conf)
        def bean = new TaskBean(task)
        bean.stageInMode = 'copy'
        return bean
    }

    def jobID(TaskId id) {
        return "${FloatConf.NF_JOB_ID}:$tJob-$id"
    }

    def submitCmd(Map param = [:]) {
        def taskIDStr = param.taskID ? param.taskID.toString() : '0'
        Integer taskID = Integer.parseInt(taskIDStr)
        def realCpu = param.cpu ?: cpu
        def realMem = param.memory ?: mem
        return [
                bin, '-a',
                param.addr ?: addr,
                '-u', user,
                '-p', pass,
                'submit',
                '--dataVolume', param.nfs ?: nfs + ':' + workDir,
                '--image', param.image ?: image,
                '--cpu', realCpu + ':' + realCpu * FloatConf.DFT_MAX_CPU_FACTOR,
                '--mem', realMem + ':' + realMem * FloatConf.DFT_MAX_MEM_FACTOR,
                param.accelerator ? "--gpu-count ${param.accelerator}": null,
                '--job', script,
                '--disableRerun',
                '--customTag', jobID(new TaskId(taskID)),
                '--customTag', "${FloatConf.NF_SESSION_ID}:uuid-$uuid",
                '--customTag', "${FloatConf.NF_TASK_NAME}:foo--$taskIDStr-",
                '--customTag', "${FloatConf.FLOAT_INPUT_SIZE}:0",
                '--customTag', "${FloatConf.NF_RUN_NAME}:test-run"
        ].findAll { it != null }
    }
}
