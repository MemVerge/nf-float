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
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

class BaseTest extends Specification {
    def setEnv(String key, String value) {
        try {
            def env = System.getenv()
            def cl = env.getClass()
            def field = cl.getDeclaredField("m")
            field.setAccessible(true)
            def writableEnv = (Map<String, String>) field.get(env)
            writableEnv.put(key, value)
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e)
        }
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
    def taskID = new TaskId(55)

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
        return exec
    }

    def newTask(FloatTestExecutor exec, TaskConfig conf = null) {
        if (conf == null) {
            conf = new TaskConfig(cpus: cpu,
                    memory: "$mem G",
                    container: image)
        }
        def task = new TaskRun()
        task.processor = Mock(TaskProcessor)
        task.processor.getSession() >> Mock(Session)
        task.processor.getExecutor() >> exec
        task.config = conf
        task.id = taskID
        return task
    }

    def jobID(TaskId id) {
        return "${FloatConf.NF_JOB_ID}:$tJob-$id"
    }

    def submitCmd(Map param = [:]) {
        return ['float', '-a', param.addr ?: addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', param.nfs ?: nfs + ':' + workDir,
                '--image', param.image ?: image,
                '--cpu', param.cpu ?: cpu.toString(),
                '--mem', param.memory ?: mem.toString(),
                '--job', script,
                '--customTag', jobID(taskID)]
    }
}
