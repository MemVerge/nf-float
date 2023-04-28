package com.memverge.nextflow

import nextflow.Session
import nextflow.executor.AbstractGridExecutor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import spock.lang.Specification

import java.nio.file.Paths

class FloatGridExecutorTest extends Specification {
    def "get the prefix of kill command"() {
        given:
        def exec = [:] as FloatGridExecutor
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
    def cpu = 5
    def mem = 10
    def image = 'cactus'
    def script = '/path/job.sh'
    def workDir = '/mnt/nfs/shared'

    def "kill command"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session
        def jobID = 'myJobID'
        sess.config = [float: [address : addr,
                               username: user,
                               password: pass]]
        exec.session = sess

        when:
        def cmd = exec.killTaskCommand(jobID)

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'scancel', '-j', jobID]
    }

    def "submit command line"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session

        sess.config = [float: [address : addr,
                               username: user,
                               password: pass]]
        String dataVolume = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs  : dataVolume,
                      cpu  : cpu,
                      mem  : mem,
                      image: image] as TaskConfig

        when:
        exec.session = sess
        task.config = config
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', dataVolume,
                '--image', image,
                '--cpu', cpu.toString(),
                '--mem', mem.toString(),
                '--job', script]
    }

    def "add default local mount point"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass]]
        task.config = [nfs  : nfs,
                       cpu  : cpu,
                       mem  : mem,
                       image: image] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', cpu.toString(),
                '--mem', mem.toString(),
                '--job', script]
    }

    def "use cpus, memory and container"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass]]
        task.config = [nfs      : nfs,
                       cpus     : 8,
                       memory   : '16 GB',
                       container: "biocontainers/star"]
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
               '-u', user,
               '-p', pass,
               'sbatch',
               '--dataVolume', nfs + ':' + workDir,
               '--image', "biocontainers/star",
               '--cpu', '8',
               '--mem', '16',
               '--job', script]
    }

    def "add common extras"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address    : addr,
                                       username   : user,
                                       password   : pass,
                                       commonExtra: '-t \t small -f ']]
        task.config = [nfs: nfs,] as TaskConfig
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
                '-t', 'small', '-f'].join(" ")
    }

    def "add specific extras"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass,]]
        task.config = [nfs  : nfs,
                       extra: ' -f -t \t small  ',] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', '2',
                '--mem', '4',
                '--job', script,
                '-f', '-t', 'small']
    }

    def "both common extra and specific extra"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address    : addr,
                                       username   : user,
                                       password   : pass,
                                       commonExtra: '-t \t small -f ']]
        task.config = [nfs  : nfs,
                       extra: '--name hello',] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', '2',
                '--mem', '4',
                '--job', script,
                '-t', 'small', '-f', '--name', 'hello']
    }

    def "config level cpu and memory"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun
        def cpu = 3
        def mem = 6

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass,
                                       cpu     : cpu,
                                       mem     : mem,
                                       nfs     : nfs]]
        task.config = [image: image] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', cpu.toString(),
                '--mem', mem.toString(),
                '--job', script]
    }

    def "use default cpu, memory and image"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session

        sess.config = [float: [address : addr,
                               username: user,
                               password: pass,
                               image   : 'cactus']]
        String dataVolume = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs: dataVolume,] as TaskConfig

        when:
        exec.session = sess
        task.config = config
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', dataVolume,
                '--image', image,
                '--cpu', '2',
                '--mem', '4',
                '--job', script]
    }

    def "use default nfs and work dir"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass,
                                       nfs     : nfs,]]
        task.config = ['cpu'  : cpu,
                       'mem'  : mem,
                       'image': image] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', cpu.toString(),
                '--mem', mem.toString(),
                '--job', script]
    }

    def "parse data volume"() {
        given:
        def exec = [:] as FloatGridExecutor
        exec.session = [:] as Session
        exec.session.workDir = Paths.get(workDir)
        def task = [:] as TaskRun

        when:
        exec.session.config = [float: [address : addr,
                                       username: user,
                                       password: pass,
                                       nfs     : nfs,]]
        task.config = ['cpu'  : cpu,
                       'mem'  : mem,
                       'image': image,
                       'extra': '''-M cpu.upperBoundDuration=5s --dataVolume "[size=50]":/BWA_BASE''',
        ] as TaskConfig
        def cmd = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd == ['float', '-a', addr,
                '-u', user,
                '-p', pass,
                'sbatch',
                '--dataVolume', nfs + ':' + workDir,
                '--image', image,
                '--cpu', cpu.toString(),
                '--mem', mem.toString(),
                '--job', script,
                '-M', 'cpu.upperBoundDuration=5s',
                '--dataVolume', '"[size=50]":/BWA_BASE']
    }

    def "parse queue status"() {
        given:
        def exec = [:] as FloatGridExecutor
        def text = """
        [                                                               
            {                                                           
                "id": "task0",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Submitted"
            },
            {                                                           
                "id": "task1",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Initializing"
            },
            {                                                           
                "id": "task2",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Executing"
            },
            {                                                           
                "id": "task3",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Floating"
            },
            {                                                           
                "id": "task4",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Completed"
            },
            {                                                           
                "id": "task5",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "Cancelled"
            },
            {                                                           
                "id": "task6",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "FailToComplete"
            },
            {                                                           
                "id": "task7",                          
                "name": "cactus-t3a.medium",                                                                                            
                "user": "admin",                                        
                "imageID": "docker.io/memverge/cactus:latest",          
                "status": "FailToExecute"
            }
        ]                          
        """.stripIndent()

        when:
        def res = exec.parseQueueStatus(text)

        then:
        def qs = AbstractGridExecutor.QueueStatus
        res.size() == 8
        res['task0'] == qs.PENDING
        res['task1'] == qs.RUNNING
        res['task2'] == qs.RUNNING
        res['task3'] == qs.HOLD
        res['task4'] == qs.DONE
        res['task5'] == qs.ERROR
        res['task6'] == qs.ERROR
        res['task7'] == qs.ERROR
    }

    def "retrieve queue status command"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session

        def address = 'float.my.com'
        def user = 'admin'
        def pass = 'password'

        sess.config = [float: [address : address,
                               username: user,
                               password: pass]]

        when:
        exec.session = sess
        def cmd = exec.queueStatusCommand(null)

        then:
        cmd == ['float', '-a', address,
                '-u', user,
                '-p', pass,
                'squeue', '--format', 'json']
    }
}
