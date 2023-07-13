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
import nextflow.executor.AbstractGridExecutor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskId
import nextflow.processor.TaskRun
import spock.lang.Specification

import java.nio.file.Paths

class FloatGridExecutorTest extends Specification {
    def setEnv(String key, String value) {
        try {
            def env = System.getenv();
            def cl = env.getClass();
            def field = cl.getDeclaredField("m");
            field.setAccessible(true);
            def writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

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

    def jobID(TaskId id) {
        return "${FloatConf.NF_JOB_ID}:$tJob-$id"
    }

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

    def "submit command line"() {
        given:
        def exec = newTestExecutor()
        String dataVolume = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs  : dataVolume,
                      cpu  : cpu,
                      mem  : mem,
                      image: image] as TaskConfig

        when:
        task.config = config
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', dataVolume,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "add default local mount point"() {
        given:
        def exec = newTestExecutor()
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [nfs  : nfs,
                       cpu  : cpu,
                       mem  : mem,
                       image: image] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "use cpus, memory and container"() {
        given:
        def exec = newTestExecutor()
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [nfs      : nfs,
                       cpus     : 8,
                       memory   : '16 GB',
                       container: "biocontainers/star"]
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', "biocontainers/star",
                          '--cpu', '8',
                          '--mem', '16',
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "add common extras"() {
        given:
        def exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         commonExtra: '-t \t small -f ']]
        )
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [nfs: nfs,] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(" ") == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', '2',
                          '--mem', '4',
                          '--job', script,
                          '--customTag', jobID(taskID),
                          '-t', 'small', '-f'].join(" ")
    }

    def "add specific extras"() {
        given:
        def exec = newTestExecutor()
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.id = taskID
        task.config = [nfs  : nfs,
                       extra: ' -f -t \t small  ',] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', '2',
                          '--mem', '4',
                          '--job', script,
                          '--customTag', jobID(taskID),
                          '-f', '-t', 'small'].join(' ')
    }

    def "both common extra and specific extra"() {
        given:
        def exec = newTestExecutor(
                [float: [address    : addr,
                         username   : user,
                         password   : pass,
                         commonExtra: '-t \t small -f ']]
        )
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [nfs  : nfs,
                       extra: '--customTag hello',] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', '2',
                          '--mem', '4',
                          '--job', script,
                          '--customTag', jobID(taskID),
                          '-t', 'small', '-f',
                          '--customTag', 'hello'].join(' ')
    }

    def "config level cpu and memory"() {
        given:
        def cpu = 3
        def mem = 6
        def exec = newTestExecutor(
                [float: [address : addr,
                         username: user,
                         password: pass,
                         cpu     : cpu,
                         mem     : mem,
                         nfs     : nfs]])
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [image: image] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "use default cpu, memory and image"() {
        given:
        def exec = newTestExecutor(
                [float: [address : addr,
                         username: user,
                         password: pass,
                         image   : 'cactus']])

        String dataVolume = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs: dataVolume,] as TaskConfig

        when:
        task.config = config
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', dataVolume,
                          '--image', image,
                          '--cpu', '2',
                          '--mem', '4',
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "use default nfs and work dir"() {
        given:
        def exec = newTestExecutor(
                [float: [address : addr,
                         username: user,
                         password: pass,
                         nfs     : nfs,]])
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = ['cpu'  : cpu,
                       'mem'  : mem,
                       'image': image] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')
    }

    def "parse data volume"() {
        given:
        def exec = newTestExecutor(
                [float: [address : addr,
                         username: user,
                         password: pass,
                         nfs     : nfs,]])
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = ['cpu'  : cpu,
                       'mem'  : mem,
                       'image': image,
                       'extra': '''-M cpu.upperBoundDuration=5s --dataVolume "[size=50]":/BWA_BASE''',
        ] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID),
                          '-M', 'cpu.upperBoundDuration=5s',
                          '--dataVolume', '"[size=50]":/BWA_BASE'].join(' ')
    }

    def "parse queue status"() {
        given:
        def exec = newTestExecutor()
        def text = """
        [                                                               
            {                                                           
                "id": "task0",                          
                "name": "tJob-0",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Submitted",
                "customTags": {
                    "nf-job-id": "tJob-0"
                }
            },
            {                                                           
                "id": "task1",                          
                "name": "tJob-1",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Initializing",
                "customTags": {
                    "nf-job-id": "tJob-1"
                }
            },
            {                                                           
                "id": "task2",                          
                "name": "tJob-2",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Executing",
                "customTags": {
                    "nf-job-id": "tJob-2"
                }
            },
            {                                                           
                "id": "task3",                          
                "name": "tJob-3",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Floating",
                "customTags": {
                    "nf-job-id": "tJob-3"
                }
            },
            {                                                           
                "id": "task4",                          
                "name": "tJob-4",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Completed",
                "customTags": {
                    "nf-job-id": "tJob-4"
                }
            },
            {                                                           
                "id": "task5",                          
                "name": "tJob-5",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Cancelled",
                "customTags": {
                    "nf-job-id": "tJob-5"
                }
            },
            {                                                           
                "id": "task6",                          
                "name": "tJob-6",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "FailToComplete",
                "customTags": {
                    "nf-job-id": "tJob-6"
                }
            },
            {                                                           
                "id": "task7",                          
                "name": "tJob-7",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "FailToExecute",
                "customTags": {
                    "nf-job-id": "tJob-7"
                }
            },
            {                                                           
                "id": "task8",                          
                "name": "tJob-8",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Completed",
                "customTags": {
                    "nf-job-id": "tJob-8"
                }
            },
            {                                                           
                "id": "task9",                          
                "name": "tJob-9",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Starting",
                "customTags": {
                    "nf-job-id": "tJob-9"
                }
            }
        ]                          
        """.stripIndent()

        when:
        def count = 9
        (0..count).forEach {
            def task = [:] as TaskRun
            task.workDir = Paths.get(
                    'src', 'test', 'com', 'memverge', 'nextflow')
            task.id = new TaskId(it)
            exec.getDirectives(task, [])
        }


        def dir = Paths.get('src', 'test', 'com', 'memverge')
        exec.floatJobs.setWorkDir(new TaskId(8), dir.toString())
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
        def exec = newTestExecutor(
                [float: [cpu: cpu,
                         mem: mem,
                         nfs: nfs]])


        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        task.config = [image: image] as TaskConfig
        task.id = taskID
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd.join(' ') == ['float', '-a', addr,
                          '-u', user,
                          '-p', pass,
                          'sbatch',
                          '--dataVolume', nfs + ':' + workDir,
                          '--image', image,
                          '--cpu', cpu.toString(),
                          '--mem', mem.toString(),
                          '--job', script,
                          '--customTag', jobID(taskID)].join(' ')

        cleanup:
        setEnv('MMC_ADDRESS', '')
        setEnv('MMC_USERNAME', '')
        setEnv('MMC_PASSWORD', '')
    }
}
