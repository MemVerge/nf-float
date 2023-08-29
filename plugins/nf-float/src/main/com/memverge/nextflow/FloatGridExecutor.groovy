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
import nextflow.exception.AbortOperationException
import nextflow.executor.AbstractGridExecutor
import nextflow.file.FileHelper
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskId
import nextflow.processor.TaskRun
import nextflow.util.Escape
import nextflow.util.ServiceName

import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.stream.Collectors

/**
 * Float Executor with a shared file system
 */
@Slf4j
@ServiceName('float')
@CompileStatic
class FloatGridExecutor extends AbstractGridExecutor {
    private static final int DFT_MEM_GB = 1
    private static final long FUSION_MIN_VOL_SIZE = 80
    private static final long MIN_VOL_SIZE = 40

    private FloatJobs _floatJobs

    private Path binDir

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
        syncFloatBin()
    }

    @Override
    FloatTaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir

        new FloatTaskHandler(task, this)
    }

    protected String getHeaderScript(TaskRun task) {
        log.info "[float] switch task ${task.id} to ${task.workDirStr}"
        floatJobs.setWorkDir(task.id, task.workDir)

        final path = Escape.path(task.workDir)
        def result = "NXF_CHDIR=${path}\n"

        if (needBinDir()) {
            // add path to the script
            result += "export PATH=\$PATH:${binDir}/bin\n"
        }

        return result
    }

    @Override
    protected String getHeaderToken() {
        null
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> initial) {
        null
    }

    private boolean needBinDir() {
        return session.binDir &&
                !session.binDir.empty() &&
                !session.disableRemoteBinDir
    }

    private void uploadBinDir() {
        if (needBinDir()) {
            binDir = getTempDir()
            log.info "Uploading local `bin` ${session.binDir} " +
                    "to ${binDir}/bin"
            FileHelper.copyPath(
                    session.binDir,
                    binDir.resolve("bin"),
                    StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private void syncFloatBin() {
        final oc = floatJobs.getOc("")
        def cmd = floatConf.getCliPrefix(oc)
        cmd << "release" << "sync"
        def res = Global.execute(cmd)
        log.info "[float] sync the float binary, $res"
    }

    private static String getMemory(TaskRun task) {
        final mem = task.config.getMemory()
        final giga = mem?.toGiga()
        if (!giga) {
            log.debug "memory $mem is too small.  " +
                    "will use default $DFT_MEM_GB"
        }
        return giga ? giga.toString() : DFT_MEM_GB
    }

    private Collection<String> getExtra(TaskRun task) {
        final extraNode = task.config.extra
        def extra = extraNode ? extraNode as String : ''
        final common = floatConf.commonExtra
        if (common) {
            extra = common.trim() + " " + extra.trim()
        }
        def ret = extra.split('\\s+')
        return ret.findAll { it.length() > 0 }
    }

    private List<String> getCmdPrefixForJob(String floatJobID) {
        final oc = floatJobs.getOc(floatJobID)
        return floatConf.getCliPrefix(oc)
    }

    FloatJob getJob(TaskId taskId) {
        def nfJobID = floatJobs.getNfJobID(taskId)
        def job = floatJobs.nfJobID2job.get(nfJobID)
        if (job == null) {
            return null
        }
        def cmd = getCmdPrefixForJob(job.floatJobID)
        cmd << 'show'
        cmd << '-j'
        cmd << job.floatJobID

        try {
            final res = Global.execute(cmd)

            if (res.succeeded) {
                job = FloatJob.parse(res.out)
            }
        } catch (Exception e) {
            log.warn "[float] failed to retrieve job status $nfJobID, float: ${job.floatJobID}", e
        }
        return job
    }

    /**
     * Retrieve the prefix of the submit command.
     * use round robin for multiple op-center
     *
     * @return
     */
    private List<String> getSubmitCmdPrefix(Integer index) {
        final addresses = floatConf.addresses
        final address = addresses[index % (addresses.size())]
        return floatConf.getCliPrefix(address)
    }

    String toLogStr(List<String> floatCmd) {
        def ret = floatCmd.join(" ")
        final toReplace = [
                ("-p " + floatConf.password): "-p ***",
                (floatConf.s3accessKey)     : "***",
                (floatConf.s3secretKey)     : "***",
        ]
        for (def entry : toReplace.entrySet()) {
            if (!entry.key) {
                continue
            }
            ret = ret.replace(entry.key, entry.value)
        }
        return ret
    }

    private static def warnDeprecated(String deprecated, String replacement) {
        log.warn1 "[float] process `$deprecated` " +
                "is no longer supported, " +
                "use $replacement instead"
    }

    private static def validate(TaskRun task) {
        if (task.config.nfs) {
            warnDeprecated('nfs', '`float.nfs` config option')
        }
        if (task.config.cpu) {
            warnDeprecated('cpu', '`cpus` directive')
        }
        if (task.config.mem) {
            warnDeprecated('mem', '`memory` directive')
        }
        if (task.config.image) {
            warnDeprecated('image', '`container` directive')
        }
    }

    private List<String> getMountVols(TaskRun task) {
        if (isFusionEnabled()) {
            return []
        }

        List<String> volumes = []
        volumes << floatConf.getWorkDirVol(workDir.uri)

        for (def src : task.getInputFilesMap().values()) {
            volumes << floatConf.getInputVolume(src.uri)
        }
        def ret = volumes.unique() - ""
        log.info "[float] volumes to mount for ${task.id}: ${toLogStr(ret)}"
        return ret
    }

    private Map<String, String> getEnv(FloatTaskHandler handler) {
        return isFusionEnabled()
                ? handler.fusionLauncher().fusionEnv()
                : [:]
    }

    private Map<String, String> getCustomTags(TaskRun task) {
        final result = new LinkedHashMap<String, String>(10)
        result[FloatConf.NF_JOB_ID] = floatJobs.getNfJobID(task.id)
        result[FloatConf.NF_SESSION_ID] = "uuid-${session.uniqueId}".toString()
        result[FloatConf.NF_TASK_NAME] = task.name
        if (task.processor.name) {
            result[FloatConf.NF_PROCESS_NAME] = task.processor.name
        }
        if (session.runName) {
            result[FloatConf.NF_RUN_NAME] = session.runName
        }
        final resourceLabels = task.config.getResourceLabels()
        if (resourceLabels)
            result.putAll(resourceLabels)
        return result
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        return getSubmitCommandLine(new FloatTaskHandler(task, this), scriptFile)
    }

    private static String toTag(String value) {
        final sizeLimit = 63
        value = value.replaceAll("[_\\s]", "-")
        value = value.replaceAll("[^a-zA-Z0-9-]", "")
        if (value.size() > sizeLimit) {
            value = value.substring(0, sizeLimit)
        }
        return value
    }

    List<String> getSubmitCommandLine(FloatTaskHandler handler, Path scriptFile) {
        final task = handler.task

        validate(task)

        final container = task.getContainer()
        if (!container) {
            throw new AbortOperationException("container is empty. " +
                    "you can specify a default container image " +
                    "with `process.container`")
        }
        def cmd = getSubmitCmdPrefix(task.index)
        cmd << 'submit'
        getMountVols(task).forEach { cmd << '--dataVolume' << it }
        cmd << '--image' << task.getContainer()
        cmd << '--cpu' << task.config.getCpus().toString()
        cmd << '--mem' << getMemory(task)
        cmd << '--job' << getScriptFilePath(handler, scriptFile)
        getEnv(handler).each { key, val ->
            cmd << '--env' << "${key}=${val}".toString()
        }
        if (isFusionEnabled()) {
            cmd << '--disableRerun'
            cmd << '--disableMigrate'
            cmd << '--extraContainerOpts'
            cmd << '--privileged'
        }
        getCustomTags(task).each { key, val ->
            cmd << '--customTag' << "${toTag(key)}:${toTag(val)}".toString()
        }
        if (task.config.getMachineType()) {
            cmd << '--instType' << task.config.getMachineType()
        }
        addVolSize(cmd, task)
        if (task.config.getTime()) {
            Float seconds = task.config.getTime().toSeconds()
            seconds *= floatConf.timeFactor
            cmd << '--timeLimit' << "${seconds.toInteger()}s".toString()
        }
        if (floatConf.vmPolicy) {
            cmd << '--vmPolicy' << floatConf.vmPolicy
        }
        if (floatConf.migratePolicy) {
            cmd << '--migratePolicy' << floatConf.migratePolicy
        }
        if (floatConf.extraOptions) {
            cmd << '--extraOptions' << floatConf.extraOptions
        }
        cmd.addAll(getExtra(task))
        log.info "[float] submit job: ${toLogStr(cmd)}"
        return cmd
    }

    private void addVolSize(List<String> cmd, TaskRun task) {
        Long size = MIN_VOL_SIZE

        def disk = task.config.getDisk()
        if (disk) {
            size = Math.max(size, disk.toGiga())
        }
        if (isFusionEnabled()) {
            size = Math.max(size, FUSION_MIN_VOL_SIZE)
        }
        if (size > MIN_VOL_SIZE) {
            cmd << '--imageVolSize' << size.toString()
        }
    }

    /**
     * TODO: should be removed when float CLI supports stdin script
     */
    private String getScriptFilePath(FloatTaskHandler handler, Path scriptFile) {
        if (isFusionEnabled()) {
            return saveFusionScriptFile(handler, scriptFile)
        }
        if (workDir.getScheme() == "s3") {
            return downloadScriptFile(scriptFile)
        }
        return scriptFile.toString()
    }

    protected static String saveFusionScriptFile(FloatTaskHandler handler, Path scriptFile) {
        final localTmp = File.createTempFile("nextflow", scriptFile.name)
        log.info("save fusion launcher script")
        localTmp.text = '#!/bin/bash\n' + handler.fusionSubmitCli().join(' ') + '\n'
        return localTmp.getAbsolutePath()
    }

    protected String downloadScriptFile(Path scriptFile) {
        final localTmp = File.createTempFile("nextflow", scriptFile.name)
        log.info("download $scriptFile to $localTmp")
        FileHelper.copyPath(
                scriptFile,
                localTmp.toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        return localTmp.getAbsolutePath()
    }

    /**
     * Parse the string returned by the {@code float submit} and extract
     * the job ID string
     *
     * @param text The string returned when submitting the job
     * @return The actual job ID string
     */
    @Override
    def parseJobId(String text) {
        return FloatJob.parse(text).floatJobID
    }

    /**
     * Kill a grid job
     *
     * @param floatJobID The ID of the job to kill,
     *        could be a string collection
     */
    @Override
    void killTask(def floatJobID) {
        def cmdList = killTaskCommands(floatJobID)
        cmdList.parallelStream().map { cmd ->
            def ret = Global.execute(cmd).exit
            if (ret != 0) {
                def m = """\
                Unable to kill pending jobs
                - cmd executed: ${toLogStr(cmd)}}
                - exit status : $ret
                - output      :
                """.stripIndent()
                log.warn m
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
    List<List<String>> killTaskCommands(def jobId) {
        def jobIds
        if (jobId instanceof Collection) {
            jobIds = jobId
        } else {
            jobIds = [jobId]
        }
        List<List<String>> ret = []
        jobIds.forEach {
            def floatJobID = it.toString()
            def cmd = getCmdPrefixForJob(floatJobID)
            cmd << 'cancel'
            cmd << '-j'
            cmd << floatJobID
            cmd << '-f'
            log.info "[float] cancel job: ${toLogStr(cmd)}"
            ret.add(cmd)
        }
        return ret
    }

    private List<String> getCmdPrefix0() {
        final addresses = floatConf.addresses
        final address = addresses.first()
        return floatConf.getCliPrefix(address)
    }

    @Override
    protected List<String> getKillCommand() {
        def cmd = getCmdPrefix0()
        cmd << 'cancel'
        cmd << '-j'
        log.info "[float] cancel job: ${toLogStr(cmd)}"
        return cmd
    }

    /**
     * @return The status for all the scheduled and running jobs
     */
    @Override
    protected Map<String, QueueStatus> getQueueStatus0(queue) {
        return queueStatus
    }

    Map<String, QueueStatus> getQueueStatus() {
        final cmdMap = queueStatusCommands()
        floatJobs.updateStatus(cmdMap)
        log.debug "[float] collecting job status completes."
        return nfJobID2Status
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
        cmd << 'list'
        cmd << '--format'
        cmd << 'json'
        log.info "[float] query job status: ${toLogStr(cmd)}"
        return cmd
    }

    private Map<String, QueueStatus> getNfJobID2Status() {
        Map<String, FloatJob> stMap = floatJobs.getNfJobID2job()
        return toStatusMap(stMap)
    }

    private static Map<String, QueueStatus> toStatusMap(Map<String, FloatJob> stMap) {
        Map<String, QueueStatus> ret = new HashMap<>()
        stMap.forEach { key, job ->
            QueueStatus status = STATUS_MAP.getOrDefault(job.status, QueueStatus.UNKNOWN)
            ret[key] = status
        }
        return ret
    }

    FloatStatus getJobStatus(TaskRun task) {
        def job = getJob(task.id)
        if (!job) {
            return FloatStatus.UNKNOWN
        }
        log.debug "[float] task id: ${task.id}, nf-job-id: $job.nfJobID, " +
                "float-job-id: $job.floatJobID, float status: $job.status"
        if (job.finished) {
            floatJobs.refreshWorkDir(job.nfJobID)
            task.exitStatus = job.rcCode
        }
        return job.status
    }

    static private Map<FloatStatus, QueueStatus> STATUS_MAP = new HashMap<>()

    static {
        STATUS_MAP.put(FloatStatus.PENDING, QueueStatus.PENDING)
        STATUS_MAP.put(FloatStatus.RUNNING, QueueStatus.RUNNING)
        STATUS_MAP.put(FloatStatus.DONE, QueueStatus.DONE)
        STATUS_MAP.put(FloatStatus.ERROR, QueueStatus.ERROR)
        STATUS_MAP.put(FloatStatus.UNKNOWN, QueueStatus.UNKNOWN)
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    boolean pipeLauncherScript() {
        return isFusionEnabled()
    }

    @Override
    boolean isFusionEnabled() {
        return FusionHelper.isFusionEnabled(session)
    }
}
