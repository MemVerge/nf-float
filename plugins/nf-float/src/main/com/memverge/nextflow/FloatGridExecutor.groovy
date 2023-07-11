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
import nextflow.executor.AbstractGridExecutor
import nextflow.extension.FilesEx
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
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

    private String _binDir

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
    protected String getHeaderToken() {
        return ''
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> initial) {
        log.info "[float] switch task ${task.id} to ${task.workDirStr}"
        floatJobs.setWorkDir(task.id, task.workDirStr)

        // go to the work directory
        initial << 'cd'
        initial << task.workDirStr

        if (needBinDir()) {
            // add path to the script
            initial << 'export'
            initial << 'PATH=$PATH:' + _binDir + '/bin'
        }
        return initial
    }

    private boolean needBinDir() {
        return session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir
    }

    private void uploadBinDir() {
        // upload local binaries
        if (needBinDir()) {
            _binDir = getTempDir()
            log.info "Uploading local `bin` ${session.binDir} to ${_binDir}/bin"
            FilesEx.copyTo(session.binDir, _binDir)
        }
    }

    private void validateTaskConf(TaskConfig config) {
        if (!config.nfs && !floatConf.nfs) {
            log.error '[float] missing "nfs": need a nfs to run float jobs'
        }
        if (!config.image && !config.container) {
            log.error '[float] missing "image": container image'
        }
        if (!config.cpu) {
            if (!config.cpus) {
                log.info '[float] missing "cpus": number of cpu cores, use default'
            }
        }
        if (!config.mem && !config.memory) {
            log.info '[float] missing "memory": size of memory in GB, use default'
        }
    }

    private String getNfs(TaskRun task) {
        def conf = task.config
        String nfs
        if (!conf.nfs) {
            nfs = floatConf.nfs
        } else {
            nfs = conf.nfs
        }
        int proto = nfs.indexOf('//')
        int colon = nfs.indexOf(':', proto + 2)
        if (colon == -1) {
            // use the workDir as local mount point
            nfs += ':' + session.workDir
        }
        return nfs
    }

    private String getCpu(TaskRun task) {
        def cpu = task.config.cpu
        if (cpu) {
            return cpu.toString()
        }
        cpu = task.config.cpus
        if (cpu) {
            return task.config.getCpus().toString()
        }
        return floatConf.cpu
    }

    private String getImage(TaskRun task) {
        def image = task.config.image
        if (image) {
            return image.toString()
        }
        image = task.config.getContainer()
        if (image) {
            return image.toString()
        }
        return floatConf.image
    }

    private String getMem(TaskRun task) {
        def mem = task.config.mem
        if (mem) {
            return mem.toString()
        }
        mem = task.config.getMemory()
        if (mem) {
            return task.config.getMemory().toGiga().toString()
        }
        return floatConf.memGB.toGiga().toString()
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

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        validateTaskConf(task.config)
        def cmd = getSubmitCmdPrefix()
        cmd << 'sbatch'
        cmd << '--dataVolume'
        cmd << getNfs(task)
        cmd << '--image'
        cmd << getImage(task)
        cmd << '--cpu'
        cmd << getCpu(task)
        cmd << '--mem'
        cmd << getMem(task)
        cmd << '--job'
        cmd << scriptFile.toString()
        cmd << '--name'
        cmd << floatJobs.getJobName(task.id)
        cmd.addAll(getExtra(task))
        log.info "[float] submit job: ${cmd.join(' ')}"
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
                - cmd executed: ${cmd.join(' ')}
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
            log.info "[float] cancel job: ${cmd.join(' ')}"
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
        log.info "[float] cancel job: ${cmd.join(' ')}"
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
        log.info "[float] query job status: ${cmd.join(' ')}"
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
}
