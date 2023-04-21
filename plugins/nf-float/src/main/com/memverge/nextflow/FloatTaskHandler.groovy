package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun

import java.nio.file.Path

/**
 * Implements a task handler for Float jobs
 */
@Slf4j
@CompileStatic
@Deprecated
class FloatTaskHandler extends TaskHandler {
    private final Path exitFile

    private final Path wrapperFile

    private final Path outputFile

    private final Path errorFile

    private final Path logFile

    private final Path scriptFile

    private final Path inputFile

    private final Path traceFile

    private FloatExecutor executor

    private FloatClient client

    private Map<String, String> environment

    private String jobId

    /** only for testing purpose -- do not use */
    protected FloatTaskHandler() {}

    FloatTaskHandler(TaskRun task, FloatExecutor executor) {
        super(task)
        this.executor = executor
        this.client = executor.client
        this.environment = System.getenv()
        this.logFile = task.workDir.resolve(TaskRun.CMD_LOG)
        this.scriptFile = task.workDir.resolve(TaskRun.CMD_SCRIPT)
        this.inputFile =  task.workDir.resolve(TaskRun.CMD_INFILE)
        this.outputFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        this.errorFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)
        this.exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        this.wrapperFile = task.workDir.resolve(TaskRun.CMD_RUN)
        this.traceFile = task.workDir.resolve(TaskRun.CMD_TRACE)
    }

    @Override
    boolean checkIfRunning() {
        log.trace "[Float] check if job=$jobId is running"
        if (!jobId || !isSubmitted())
            return false
        final res = client.query(jobId)
        return res.isJobRunning()
    }

    @Override
    boolean checkIfCompleted() {
        log.trace "[Float] check if job=$jobId is completed"
        return !checkIfRunning()
    }

    @Override
    void kill() {
        log.trace "[Float] killing job=$jobId"
        assert jobId
        client.kill(jobId)
    }

    @Override
    void submit() {
        buildTaskWrapper()
        // TODO:
    }

    protected BashWrapperBuilder createTaskWrapper() {
        return new FloatScriptLauncher(task.toTaskBean(), getOptions())
    }

    protected void buildTaskWrapper() {
        createTaskWrapper().build()
    }

    protected String getJobId() {jobId}

    protected FloatOptions getOptions() {executor.getOptions()}
}
