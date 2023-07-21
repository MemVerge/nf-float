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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.ProcessUnrecoverableException
import nextflow.executor.AbstractGridExecutor
import nextflow.extension.FilesEx
import nextflow.processor.TaskRun
import nextflow.util.Escape
import nextflow.util.ServiceName
import org.apache.commons.lang.StringUtils

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

/**
 * Float Executor with a shared file system
 */
@Slf4j
@ServiceName('float')
@CompileStatic
class FloatGridExecutor extends AbstractGridExecutor {
    private FloatJobs _floatJobs

    private AtomicInteger serial = new AtomicInteger()

    private String binDir

    private FloatConf getFloatConf() {
        return FloatConf.getConf(session.config)
    }

    FloatJobs getFloatJobs() {
        if (_floatJobs == null) {
            _floatJobs = new FloatJobs(floatConf.addresses)
        }
        return _floatJobs
    }

    @Override
    protected void register() {
        super.register()
        uploadBinDir()
    }

    @Override
    FloatTaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir

        new FloatTaskHandler(task, this)
    }

    protected String getHeaderScript(TaskRun task) {
        log.info "[float] switch task ${task.id} to ${task.workDirStr}"
        floatJobs.setWorkDir(task.id, task.workDirStr)

        def result = ""
        result += "NXF_CHDIR=${Escape.path(task.workDir)}\n"

        if (needBinDir()) {
            // add path to the script
            result += "export PATH=\$PATH:${binDir}/bin\n"
        }

        return result
    }

    @Override
    protected String getHeaderToken() { null }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> initial) { null }

    private boolean needBinDir() {
        return session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir
    }

    private void uploadBinDir() {
        // upload local binaries
        if (needBinDir()) {
            binDir = getTempDir()
            log.info "Uploading local `bin` ${session.binDir} to ${binDir}/bin"
            FilesEx.copyTo(session.binDir, binDir)
        }
    }

    private String getDataVolume(TaskRun task) {
        // use config option if specified
        if (floatConf.nfs)
            return floatConf.nfs

        // otherwise use work directory
        final workDir = getWorkDir()

        if (workDir.scheme == 's3') {
            // insert AWS credentials if they are available
            final accessKey = session.config.navigate('aws.accessKey')
            final secretKey = session.config.navigate('aws.secretKey')
            if (accessKey && secretKey)
                return "[accesskey=${accessKey},secret=${secretKey},mode=rw]${workDir.toUriString()}:${workDir}"
        }

        return workDir.toUriString()
    }

    private String getMemory(TaskRun task) {
        final mem = task.config.getMemory()
        return mem ? mem.toGiga().toString() : '4'
    }

    private Collection<String> getExtra(TaskRun task) {
        def extraNode = task.config.extra
        def extra = extraNode ? extraNode as String : ''
        def common = floatConf.commonExtra
        if (StringUtils.length(common)) {
            extra = common.trim() + " " + extra.trim()
        }
        def ret = extra.split('\\s+')
        return ret.findAll { it.length() > 0 }
    }

    private List<String> getCmdPrefixForJob(String jobID) {
        def oc = floatJobs.getOc(jobID)
        return floatConf.getCliPrefix(oc)
    }

    /**
     * Retrieve the prefix of the submit command.
     * use round robin for multiple op-center
     *
     * @return
     */
    private List<String> getSubmitCmdPrefix() {
        def i = serial.incrementAndGet()
        def addresses = floatConf.addresses
        def address = addresses[i % (addresses.size())]
        return floatConf.getCliPrefix(address)
    }

    private String toCmdString(List<String> floatCmd) {
        def ret = floatCmd.join(" ")
        return ret.replace("-p ${floatConf.password}", "-p ***")
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        if (task.config.nfs)
            log.warn '[float] process `nfs` is no longer supported, use `float.nfs` config option instead'
        if (task.config.cpu)
            log.warn '[float] process `cpu` is no longer supported, use `cpus` directive instead'
        if (task.config.mem)
            log.warn '[float] process `mem` is no longer supported, use `memory` directive instead'
        if (task.config.image)
            log.warn '[float] process `image` is no longer supported, use `container` directive instead'

        final container = task.getContainer()
        if (!container)
            throw new ProcessUnrecoverableException("Process `${task.lazyName()}` failed because the container image was not specified")

        String tag = "${FloatConf.NF_JOB_ID}:${floatJobs.getJobName(task.id)}"

        def cmd = getSubmitCmdPrefix()
        cmd << 'sbatch'
        cmd << '--dataVolume' << getDataVolume(task)
        cmd << '--image' << container
        cmd << '--cpu' << task.config.getCpus().toString()
        cmd << '--mem' << getMemory(task)
        cmd << '--job' << scriptFile.toUriString()
        cmd << '--customTag' << tag
        cmd.addAll(getExtra(task))
        log.info "[float] submit job: ${toCmdString(cmd)}"
        return cmd
    }

    /**
     * Parse the string returned by the {@code float sbatch} and extract
     * the job ID string
     *
     * @param text The string returned when submitting the job
     * @return The actual job ID string
     */
    @Override
    def parseJobId(String text) {
        return CmdResult.with(text).jobID()
    }

    /**
     * Kill a grid job
     *
     * @param jobId The ID of the job to kill,
     *        could be a string collection
     */
    @Override
    void killTask(def jobId) {
        def cmdList = killTaskCommands(jobId)
        cmdList.parallelStream().map { cmd ->
            def proc = new ProcessBuilder(cmd).redirectErrorStream(true).start()
            proc.waitForOrKill(10_000)
            def ret = proc.exitValue()
            if (ret != 0) {
                def m = """\
                Unable to kill pending jobs
                - cmd executed: ${toCmdString(cmd)}}
                - exit status : $ret
                - output      :
                """.stripIndent()
                m += proc.text.indent('  ')
                log.debug(m)
            }
            return ret
        }.collect()
    }

    /**
     * The command to be used to kill a grid job
     *
     * @param jobId The job ID to be kill
     * @return The command line to be used to kill the specified job
     */
    protected List<List<String>> killTaskCommands(def jobId) {
        def jobIds
        if (jobId instanceof Collection) {
            jobIds = jobId
        } else {
            jobIds = [jobId]
        }
        List<List<String>> ret = []
        jobIds.forEach {
            def id = it.toString()
            def cmd = getCmdPrefixForJob(id)
            cmd << 'scancel'
            cmd << '-j'
            cmd << id
            log.info "[float] cancel job: ${toCmdString(cmd)}"
            ret.add(cmd)
        }
        return ret
    }

    private List<String> getCmdPrefix0() {
        def addresses = floatConf.addresses
        def address = addresses.first()
        return floatConf.getCliPrefix(address)
    }

    @Override
    protected List<String> getKillCommand() {
        def cmd = getCmdPrefix0()
        cmd << 'scancel'
        cmd << '-j'
        log.info "[float] cancel job: ${toCmdString(cmd)}"
        return cmd
    }

    /**
     * @return The status for all the scheduled and running jobs
     */
    @Override
    protected Map<String, QueueStatus> getQueueStatus0(queue) {
        Map<String, List<String>> cmdMap = queueStatusCommands()
        floatJobs.updateStatus(cmdMap)
        log.debug "[float] collecting job status completes."
        return queueStatus
    }

    protected Map<String, List<String>> queueStatusCommands() {
        return floatConf.addresses.stream().collect(
                Collectors.toMap(
                        oc -> oc.toString(),
                        oc -> getQueueCmdOfOC(oc.toString())))
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        return getQueueCmdOfOC()
    }

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String s) {
        def stMap = floatJobs.parseQStatus(s)
        return toStatusMap(stMap)
    }

    private List<String> getQueueCmdOfOC(String oc = "") {
        def cmd = floatConf.getCliPrefix(oc)
        cmd << 'squeue'
        cmd << '--format'
        cmd << 'json'
        log.info "[float] query job status: ${toCmdString(cmd)}"
        return cmd
    }

    private Map<String, QueueStatus> getQueueStatus() {
        Map<String, String> stMap = floatJobs.getJob2Status()
        return toStatusMap(stMap)
    }

    private static Map<String, QueueStatus> toStatusMap(Map<String, String> stMap) {
        Map<String, QueueStatus> ret = new HashMap<>()
        stMap.forEach { key, value ->
            QueueStatus status = STATUS_MAP.getOrDefault(value, QueueStatus.UNKNOWN)
            ret[key] = status
        }
        return ret
    }

    static private Map<String, QueueStatus> STATUS_MAP = [
            'Submitted'        : QueueStatus.PENDING,
            'Initializing'     : QueueStatus.PENDING,
            'Starting'         : QueueStatus.RUNNING,
            'Executing'        : QueueStatus.RUNNING,
            'Floating'         : QueueStatus.RUNNING,
            'Completed'        : QueueStatus.DONE,
            'Cancelled'        : QueueStatus.ERROR,
            'Cancelling'       : QueueStatus.ERROR,
            'FailToComplete'   : QueueStatus.ERROR,
            'FailToExecute'    : QueueStatus.ERROR,
            'CheckpointFailed' : QueueStatus.ERROR,
            'WaitingForLicense': QueueStatus.ERROR,
            'Timedout'         : QueueStatus.ERROR,
            'NoAvailableHost'  : QueueStatus.ERROR,
            'Unknown'          : QueueStatus.UNKNOWN,
    ]

    @Override
    boolean isContainerNative() {
        return true
    }
}
