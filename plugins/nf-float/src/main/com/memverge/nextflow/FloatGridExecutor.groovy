package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.AbstractGridExecutor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import nextflow.util.ServiceName
import org.apache.commons.lang.StringUtils

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

/**
 * Float Executor with a shared file system
 */
@Slf4j
@ServiceName('float')
@CompileStatic
class FloatGridExecutor extends AbstractGridExecutor {
    private Map<String, String> job2oc = new ConcurrentHashMap<>()
    private AtomicInteger serial = new AtomicInteger()

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
        def oc = job2oc.getOrDefault(jobID, floatConf.addresses[0])
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
        if (cmdMap.size() == 0) {
            return null
        }
        def ret = new ConcurrentHashMap<String, QueueStatus>()
        cmdMap.entrySet().parallelStream().map { entry ->
            def oc = entry.key
            def cmd = entry.value
            log.debug "[float] getting queue ${queue ? "($queue) " : ''}status > cmd: ${cmd.join(' ')}"
            try {
                final buf = new StringBuilder()
                final process = new ProcessBuilder(cmd).redirectErrorStream(true).start()
                final consumer = process.consumeProcessOutputStream(buf)
                process.waitForOrKill(60_000)
                final exit = process.exitValue(); consumer.join() // <-- make sure sync with the output consume #1045
                final result = buf.toString()

                if (exit == 0) {
                    log.trace "[${name.toUpperCase()}] queue ${queue ? "($queue) " : ''}status > cmd exit: $exit\n$result"
                    def status = parseQueueStatus(result)
                    status.each { key, val ->
                        ret[key] = val
                        job2oc[key] = oc
                    }
                } else {
                    def m = """\
                [float] queue ${queue ? "($queue) " : ''}status cannot be fetched
                - cmd executed: ${cmd.join(' ')}
                - exit status : $exit
                - output      :
                """.stripIndent()
                    m += result.indent('  ')
                    log.warn1(m, firstOnly: true)
                }
            } catch (Exception e) {
                log.warn "[${name.toUpperCase()}] failed to retrieve queue ${queue ? "($queue) " : ''}status -- See the log file for details", e
            }
        }.collect()
        log.debug "[float] collecting job status completes."
        return ret
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

    private List<String> getQueueCmdOfOC(String oc = "") {
        def cmd = floatConf.getCliPrefix(oc)
        cmd << 'squeue'
        cmd << '--format'
        cmd << 'json'
        log.info "[float] query job status: ${cmd.join(' ')}"
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
