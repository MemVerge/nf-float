package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.AbstractGridExecutor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import nextflow.util.ServiceName
import org.apache.commons.lang.StringUtils

import java.nio.file.Path

/**
 * Float Executor with a shared file system
 */
@Slf4j
@ServiceName('float')
@CompileStatic
class FloatGridExecutor extends AbstractGridExecutor {

    private FloatClient getClient() {
        final factory = new FloatClientFactory(session.config)
        return factory.getFloatClient()
    }

    private FloatConf getFloatConf() {
        return FloatConf.getConf(session.config)
    }

    @Override
    protected String getHeaderToken() {
        return ''
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> initial) {
        log.info "[float] switch to ${task.workDirStr}"
        initial << 'cd'
        initial << task.workDirStr
        return initial
    }

    private void validateTaskConf(TaskConfig config) {
        if (!config.nfs && !floatConf.nfs) {
            log.error '[float] missing "nfs": need a nfs to run float jobs'
        }
        if (!config.image) {
            log.error '[float] missing "image": container image'
        }
        if (!config.cpu) {
            log.info '[float] missing "cpus": number of cpu cores, use default'
        }
        if (!config.mem) {
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
        return floatConf.cpu
    }

    private String getImage(TaskRun task) {
        def image = task.config.image
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
        return floatConf.memGB
    }

    private Collection<String> getExtra(TaskRun task) {
        def extraNode = task.config.extra
        def extra = extraNode ? extraNode as String : ''
        def common = floatConf.commonExtra
        if (StringUtils.length(common)) {
            extra = common.trim() + " " + extra.trim()
        }
        def ret = extra.split('\\s+')
        return ret.findAll {it.length() > 0}
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        validateTaskConf(task.config)
        def cmd = client.getCmdPrefix()
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

    @Override
    protected List<String> getKillCommand() {
        def cmd = client.getCmdPrefix()
        cmd << 'scancel'
        cmd << '-j'
        return cmd
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        def cmd = client.getCmdPrefix()
        cmd << 'squeue'
        cmd << '--format'
        cmd << 'json'
        return cmd
    }

    static private Map<String, QueueStatus> STATUS_MAP = [
            "Submitted"        : QueueStatus.PENDING,
            "Initializing"     : QueueStatus.RUNNING,
            "Executing"        : QueueStatus.RUNNING,
            "Floating"         : QueueStatus.HOLD,
            "Completed"        : QueueStatus.DONE,
            "Cancelled"        : QueueStatus.ERROR,
            "FailToComplete"   : QueueStatus.ERROR,
            "FailToExecute"    : QueueStatus.ERROR,
            "CheckpointFailed" : QueueStatus.ERROR,
            "WaitingForLicense": QueueStatus.ERROR,
            "Timedout"         : QueueStatus.ERROR,
            "NoAvailableHost"  : QueueStatus.ERROR,
            "Unknown"          : QueueStatus.ERROR,
    ]

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        Map<String, QueueStatus> ret = [:]
        def stMap = CmdResult.with(text).getQStatus()
        stMap.each { key, value ->
            def sKey = key as String
            def st = STATUS_MAP.get(value)
            if (sKey && st) {
                ret.put(key as String, STATUS_MAP.get(value))
            }
        }
        return ret
    }
}
