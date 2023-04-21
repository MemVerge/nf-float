/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.extension.FilesEx
import nextflow.processor.ParallelPollingMonitor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.RateUnit
import nextflow.util.ServiceName
import nextflow.util.ThrottlingExecutor
import org.pf4j.ExtensionPoint

import java.nio.file.Path
import java.util.concurrent.TimeUnit

@Slf4j
@ServiceName('float-full')
@CompileStatic
@Deprecated
class FloatExecutor extends Executor implements ExtensionPoint{
    private Map<String, String> sysEnv = System.getenv()

    private FloatClient client

    private FloatOptions options

    FloatOptions getOptions() {options}

    /**
     * executor service to throttle service requests
     */
    private ThrottlingExecutor submitter

    /**
     * path to a cloud drive where executable scripts ned to be uploaded
     */
    private Path remoteBinDir = null

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if (session.binDir &&
                !session.binDir.empty()&&
                !session.disableRemoteBinDir) {
            def remote = getTempDir()
            log.info "Send local bin scripts to ${remote.toUriString()}/bin"
            remoteBinDir = FilesEx.copyTo(session.binDir, remote)
        }
    }

    protected void createFloatClient() {
        // retrieve config and credentials and create the client
        final factory = new FloatClientFactory(session.config)

        // create a float client from the factory
        client = factory.getFloatClient()
        options = new FloatOptions(this)
        log.debug "[Float] executor options=${options}"
    }

    @PackageScope
    Path getRemoteBinDir() {
        remoteBinDir
    }

    @PackageScope
    FloatClient getClient() {
        client
    }

    protected validateWorkDir() {
        // do nothing
    }

    protected validatePathDir() {
        if (session.config.navigate('env.PATH')) {
            log.warn "environment PATH in the config is ignored"
        }
    }

    /**
     * Initialise the float executor.
     */
    @Override
    protected void register() {
        super.register()
        validateWorkDir()
        validatePathDir()
        uploadBinDir()
        createFloatClient()
    }

    @Override
    protected TaskMonitor createTaskMonitor() {
        // create the throttling executor
        // note this is invoke only the very first time
        // a float executor is created
        //therefore it's safe to assign a static attribute
        submitter = createExecutorService('MMCE-executor')

        final pollInterval = session.getPollInterval(name, Duration.of('30 sec'))
        final dumpInterval = session.getMonitorDumpInterval(name)
        final capacity = session.getQueueSize(name, 500)

        final def params = [
                name: name,
                session: session,
                pollInterval: pollInterval,
                dumpInterval: dumpInterval,
                capacity: capacity
        ]

        log.debug "Creating parallel monitor for executor '$name' > pollInterval=$pollInterval; dumpInterval=$dumpInterval"
        return new ParallelPollingMonitor(submitter, params)
    }

    /**
     * Create a task handler for the given task instance
     *
     * @param task The {@link TaskRun} instance to be executed
     * @return A {@link FloatTaskHandler} for the given task
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.trace "[Float] launching process > ${task.name}, work folder: ${task.workDirStr}"
        return new FloatTaskHandler(task, this)
    }

    /**
     * @param name of the service
     * @return Creates a {@link ThrottlingExecutor} service to throttle
     * the requests to the MMCE service
     */
    private ThrottlingExecutor createExecutorService(String name) {
        // queue size can be overridden by submitter options below
        final qs = 5_000
        final limit = session.getExecConfigProp(name,'submitRateLimit','50/s') as String
        final size = Runtime.runtime.availableProcessors() * 5

        final opts = new ThrottlingExecutor.Options()
                .onFailure { Throwable t -> session?.abort(t) }
                .onRateLimitChange { RateUnit rate -> logRateLimitChange(rate) }
                .withRateLimit(limit)
                .withQueueSize(qs)
                .withPoolSize(size)
                .withKeepAlive(Duration.of('1 min'))
                .withAutoThrottle(true)
                .withMaxRetries(10)
                .withOptions( getConfigOpts() )
                .withPoolName(name)

        ThrottlingExecutor.create(opts)
    }

    @CompileDynamic
    protected Map getConfigOpts() {
        session.config?.executor?.submitter as Map
    }

    protected static void logRateLimitChange(RateUnit rate) {
        log.debug "new submission rate limit: $rate"
    }

    @Override
    void shutdown() {
        def tasks = submitter.shutdownNow()
        if (tasks) log.warn "execution interrupted -- cleaning up execution pool"
        submitter.awaitTermination(5, TimeUnit.MINUTES)
        // -- finally delete cleanup executor
        // start shutdown process
    }
}
