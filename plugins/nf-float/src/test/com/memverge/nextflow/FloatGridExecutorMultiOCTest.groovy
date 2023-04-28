package com.memverge.nextflow

import nextflow.Session
import nextflow.processor.TaskConfig
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
    def image = 'cactus'
    def script = '/path/job.sh'

    def "submit job with round robin"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session

        sess.config = [float: [address : addr,
                               username: user,
                               password: pass]]
        def dataVol = nfs + ':/data'
        def task = [:] as TaskRun
        def config = [nfs  : dataVol,
                      cpu  : cpu,
                      mem  : mem,
                      image: image] as TaskConfig

        when:
        exec.session = sess
        task.config = config
        def cmd1 = exec.getSubmitCommandLine(task, Paths.get(script))
        def cmd2 = exec.getSubmitCommandLine(task, Paths.get(script))

        then:
        cmd1.join(' ') == "float -a fb -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}"
        cmd2.join(' ') == "float -a fa -u admin -p password sbatch " +
                "--dataVolume ${dataVol} --image ${image} " +
                "--cpu ${cpu} --mem ${mem} --job ${script}"
    }

    def "get queue status commands"() {
        given:
        def exec = [:] as FloatGridExecutor
        def sess = [:] as Session

        sess.config = [float: [address : addr,
                               username: user,
                               password: pass]]

        when:
        exec.session = sess
        def cmdMap = exec.queueStatusCommands()
        def cmd1 = cmdMap['fa']
        def cmd2 = cmdMap['fb']

        then:
        cmd1.join(' ') == "float -a fa -u ${user} -p ${pass} " +
                "squeue --format json"
        cmd2.join(' ') == "float -a fb -u ${user} -p ${pass} " +
                "squeue --format json"
    }
}
