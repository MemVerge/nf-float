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
import nextflow.executor.BashWrapperBuilder
import nextflow.file.FileHelper
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskBean
import nextflow.processor.TaskId
import nextflow.processor.TaskRun
import nextflow.util.Escape
import nextflow.util.ServiceName

import java.nio.file.Path
import java.nio.file.StandardCopyOption

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
    private static final long DISK_INPUT_FACTOR = 5

    private FloatJobs _floatJobs

    private Path binDir

    private FloatConf getFloatConf() {
        return FloatConf.getConf(session.config)
    }

    synchronized FloatJobs getFloatJobs() {
        if (_floatJobs == null) {
            _floatJobs = new FloatJobs(getRunName())
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

    protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        final bean = new TaskBean(task)
        final strategy = new FloatFileCopyStrategy(floatConf, bean)
        // creates the wrapper script
        final builder = new BashWrapperBuilder(bean, strategy)
        // job directives headers
        builder.headerScript = getHeaderScript(task)
        return builder
    }

    protected String getHeaderScript(TaskRun task) {
        log.info "[FLOAT] switch task ${task.id} to ${task.workDirStr}"
        floatJobs.setWorkDir(task.id, task.workDir)

        final path = Escape.path(task.workDir)
        def result = "NXF_CHDIR=${path}\n"

        if (needBinDir()) {
            // add path to the script
            result += "export PATH=\$PATH:${binDir}/bin\n"
        }

        if (floatConf.s3cred.isValid()) {
            // if we have s3 credential, make sure aws cli is available in path
            result += "export PATH=\$PATH:/opt/aws/dist\n"
            result += "export LD_LIBRARY_PATH=\$LIBRARY_PATH:/opt/aws/dist\n"
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
        def cmd = floatConf.getCliPrefix(null)
        cmd << "release" << "sync"
        def res = Global.execute(cmd)
        log.info "[FLOAT] sync the float binary, $res"
    }

    private int getMemory(TaskRun task) {
        final mem = task.config.getMemory()
        Long giga = mem?.toGiga()
        if (!giga) {
            log.debug "memory $mem is too small.  " +
                    "will use default $DFT_MEM_GB"
            giga = DFT_MEM_GB
        }
        giga = (long) ((float) (giga) * floatConf.memoryFactory)
        giga = Math.max(giga, DFT_MEM_GB)
        return giga
    }

    private Collection<String> getExtra(TaskRun task) {
        List<String> extras = []
        final common = floatConf.commonExtra
        if (common) {
            extras.add(common.trim())
        }
        final extraNode = task.config.extra
        if (extraNode) {
            extras.add((extraNode as String).trim())
        }
        final extFloat = task.config.ext?['float']
        if (extFloat) {
            extras.add((extFloat as String).trim())
        }
        def ret = splitWithQuotes(extras.join(' '))
        return ret.findAll { it.length() > 0 }
    }

    private static List<String> splitWithQuotes(String input) {
        List<String> ret = new ArrayList<String>()
        int start = 0
        boolean inQuotes = false
        for (int i = 0; i < input.size(); i++) {
            if (input[i] == '"') {
                inQuotes = !inQuotes
            }
            if ((input[i] == '\t' || input[i] == ' ') && !inQuotes) {
                String token = input.substring(start, i).trim()
                if (token.size() > 0) {
                    ret.add(token)
                }
                start = i
            }
        }
        String token = input.substring(start).trim()
        if (token.size() > 0) {
            ret.add(token)
        }
        return ret
    }

    FloatJob getJob(TaskId taskId) {
        def nfJobID = floatJobs.getNfJobID(taskId)
        def job = floatJobs.getJob(taskId)
        if (job == null) {
            log.warn "[FLOAT] job not found for nextflow task $taskId: $nfJobID"
            return null
        }
        def cmd = floatConf.getCliPrefix(taskId)
        cmd << 'show'
        cmd << '-j'
        cmd << job.floatJobID

        try {
            final res = Global.execute(cmd)

            if (res.succeeded) {
                job = FloatJob.parse(res.out)
                job = floatJobs.updateJob(job)
            } else {
                log.warn "[FLOAT] failed to retrieve job status $nfJobID, " +
                        "float: ${job.floatJobID}, details: ${res.out}"
            }
        } catch (Exception e) {
            log.warn "[FLOAT] failed to retrieve job status $nfJobID, " +
                    "float: ${job.floatJobID}", e
        }
        return job
    }

    private static def warnDeprecated(String deprecated, String replacement) {
        log.warn1 "[FLOAT] process `$deprecated` " +
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
        log.info "[FLOAT] volumes to mount for ${task.id}: ${floatConf.toLogStr(ret)}"
        return ret
    }

    // get the size of the input files in bytes
    private static long getInputFileSize(TaskRun task) {
        long ret = 0
        for (def src : task.getInputFilesMap().values()) {
            ret += src.size()
        }
        return ret
    }

    private Map<String, String> getEnv(FloatTaskHandler handler) {
        def ret = isFusionEnabled()
                ? handler.fusionLauncher().fusionEnv()
                : [:]
        return floatConf.s3cred.updateEnvMap(ret)
    }

    String getRunName() {
        // replace _ with - to avoid float cli error
        return session.runName
                .replaceAll("_", "-")
                .toLowerCase()
    }

    private Map<String, String> getCustomTags(TaskRun task) {
        final result = new LinkedHashMap<String, String>(10)
        def nfJobID = floatJobs.getNfJobID(task.id)
        result[FloatConf.NF_JOB_ID] = nfJobID
        result[FloatConf.NF_SESSION_ID] = "uuid-${session.uniqueId}".toString()
        result[FloatConf.NF_TASK_NAME] = task.name
        result[FloatConf.FLOAT_INPUT_SIZE] = getInputFileSize(task).toString()

        final processName = task.processor.name
        if (processName) {
            result[FloatConf.NF_PROCESS_NAME] = processName
            result[FloatConf.FLOAT_JOB_KIND] = getJobKind(processName)
        }
        if (session.workflowMetadata) {
            final projName = session.workflowMetadata.projectName
            if (projName) {
                result[FloatConf.NF_PROJECT_NAME] = projName
            }
        }
        def runName = getRunName()
        if (runName) {
            result[FloatConf.NF_RUN_NAME] = runName
        }
        final resourceLabels = task.config.getResourceLabels()
        if (resourceLabels)
            result.putAll(resourceLabels)
        return result
    }

    private String getJobKind(String processName) {
        def fsPrefix = workDir.getScheme()
        if (isFusionEnabled()) {
            fsPrefix += 'fu'
        }
        return fsPrefix + '-' + processName
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        return getSubmitCommandLine(new FloatTaskHandler(task, this), scriptFile)
    }

    private static String toTag(String value) {
        final sizeLimit = 63
        value = value.replaceAll("[_\\s]", "-")
        value = value.replaceAll("[^a-zA-Z0-9-]", "-")
        if (value.size() > sizeLimit) {
            value = value.substring(0, sizeLimit)
        }
        return value
    }

    private Integer getCpu(TaskRun task) {
        int cpu = task.config.getCpus()
        int ret = (int) (((float) cpu) * floatConf.cpuFactor)
        return Math.max(ret, 1)
    }

    List<String> getSubmitCommandLine(FloatTaskHandler handler, Path scriptFile) {
        final task = handler.task

        validate(task)

        final container = task.getContainer().strip(" \t\'\"")
        if (!container) {
            throw new AbortOperationException("container is empty. " +
                    "you can specify a default container image " +
                    "with `process.container`")
        } else {
            log.info("got container image of the task ${container}")
        }
        def cmd = floatConf.getCliPrefix(task.id)
        cmd << 'submit'
        getMountVols(task).forEach { cmd << '--dataVolume' << it }
        cmd << '--image' << container

        int cpu = getCpu(task)
        int maxCpu = (floatConf.maxCpuFactor * cpu.doubleValue()).intValue()
        cmd << '--cpu' << "${cpu}:${maxCpu}".toString()
        int memGiga = getMemory(task)
        int maxMemGiga = (floatConf.maxMemoryFactor * memGiga.doubleValue()).intValue()
        cmd << '--mem' << "${memGiga}:${maxMemGiga}".toString()
        cmd << '--job' << getScriptFilePath(handler, scriptFile)
        getEnv(handler).each { key, val ->
            cmd << '--env' << "${key}=${val}".toString()
        }
        cmd << '--disableRerun'
        if (isFusionEnabled()) {
            cmd << '--disableMigrate'
            cmd << '--extraContainerOpts'
            cmd << '--privileged'
        }
        def tags = getCustomTags(task)
        tags.each { key, val ->
            cmd << '--customTag' << "${toTag(key)}:${toTag(val)}".toString()
        }
        if (task.config.getMachineType()) {
            cmd << '--instType' << task.config.getMachineType()
        }
        addVolSize(cmd, task)
        if (!floatConf.ignoreTimeFactor && task.config.getTime()) {
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
        if (task.config.getAttempt() > 1) {
            cmd << '--vmPolicy' << '[onDemand=true]'
        }
        cmd.addAll(getExtra(task))
        log.info "[FLOAT] submit job: ${floatConf.toLogStr(cmd)}"
        return cmd
    }

    private void addVolSize(List<String> cmd, TaskRun task) {
        long size = MIN_VOL_SIZE

        def disk = task.config.getDisk()
        if (disk) {
            size = Math.max(size, disk.toGiga())
        }
        if (isFusionEnabled()) {
            size = Math.max(size, FUSION_MIN_VOL_SIZE)
        }
        long inputSizeGB =  (long)(getInputFileSize(task) / 1024 / 1024 / 1024)
        long minDiskSizeBasedOnInput = inputSizeGB * DISK_INPUT_FACTOR
        size = Math.max(size, minDiskSizeBasedOnInput)
        cmd << '--imageVolSize' << size.toString()
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
        def job = FloatJob.parse(text)
        floatJobs.updateJob(job)
        return job.floatJobID
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
                - cmd executed: ${floatConf.toLogStr(cmd)}}
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
        def floatJobIDs
        if (jobId instanceof Collection) {
            floatJobIDs = jobId
        } else {
            floatJobIDs = [jobId]
        }
        List<List<String>> ret = []
        for (def it : floatJobIDs) {
            def floatJobID = it.toString()
            def taskID = floatJobs.getTaskID(floatJobID)
            if (taskID == null) {
                log.warn "[FLOAT] task id not found for float job id: $floatJobID"
                return
            }
            def cmd = floatConf.getCliPrefix(taskID)
            cmd << 'cancel'
            cmd << '-j'
            cmd << floatJobID
            cmd << '-f'
            log.info "[FLOAT] cancel job: ${floatConf.toLogStr(cmd)}"
            ret.add(cmd)
        }
        return ret
    }

    @Override
    protected List<String> getKillCommand() {
        def cmd = floatConf.getCliPrefix(null)
        cmd << 'cancel'
        cmd << '-j'
        log.info "[FLOAT] cancel job: ${floatConf.toLogStr(cmd)}"
        return cmd
    }

    /**
     * @return The status for all the scheduled and running jobs
     */
    @Override
    protected Map<String, QueueStatus> getQueueStatus0(_) {
        log.debug "[FLOAT] collecting job status completes."
        return toStatusMap(floatJobs.getNfJobID2Job())
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        return []
    }

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String s) {
        return getQueueStatus0(null)
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
            log.info "[FLOAT] task status unknown, job not found for ${task.id}"
            return FloatStatus.UNKNOWN
        }
        log.info "[FLOAT] task id: ${task.id}, nf-job-id: $job.nfJobID, " +
                "float-job-id: $job.floatJobID, float status: $job.status"
        return job.status
    }

    Integer getJobRC(TaskId id) {
        def job = getJob(id)
        return job.rcCode
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
