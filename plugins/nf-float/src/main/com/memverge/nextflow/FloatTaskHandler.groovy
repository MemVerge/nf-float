/*
 * Copyright 2013-2023, Seqera Labs
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
import nextflow.executor.GridTaskHandler
import nextflow.processor.TaskRun

import java.nio.file.FileSystems

import static nextflow.processor.TaskStatus.COMPLETED

/**
 * Float task handler
 */
@Slf4j
@CompileStatic
class FloatTaskHandler extends GridTaskHandler {

    FloatTaskHandler(TaskRun task, FloatGridExecutor executor) {
        super(task, executor)
    }

    private FloatGridExecutor getFloatExecutor() {
        return ((FloatGridExecutor) executor)
    }

    /**
     * Override the grid task handler to never create the task directory.
     */
    @Override
    protected ProcessBuilder createProcessBuilder() {

        // -- log the submit command
        final cli = floatExecutor.getSubmitCommandLine(this, wrapperFile)
        log.trace "start process ${task.name} > cli: ${cli}"

        // -- prepare submit process
        final builder = new ProcessBuilder()
                .command(cli as String[])
                .redirectErrorStream(true)

        if (task.workDir.fileSystem == FileSystems.default) {
            builder.directory(task.workDir.toFile())
        }

        return builder
    }

    /**
     * Override the grid task handler to make the fusion launcher
     * script container-native.
     */
    protected String fusionStdinWrapper() {
        return '#!/bin/bash\n' + fusionSubmitCli().join(' ') + '\n'
    }

    @Override
    boolean checkIfCompleted() {
        final FloatStatus st =  floatExecutor.getJobStatus(task)
        log.debug "got status ${st} for ${task.id} from float executor"
        if (st.finished) {
            status = COMPLETED
            task.exitStatus = readExitStatus()
            if (task.exitStatus == null) {
                log.debug "can't get ${task.id} exit status from file system"
                task.exitStatus = floatExecutor.getJobRC(task.id)
            }
            log.debug "set ${task.id} exit status to ${task.exitStatus}"
            if (task.exitStatus == null) {
                if (st.isError()) {
                    task.exitStatus = 1
                } else {
                    task.exitStatus = 0
                }
                log.info "both .exitcode and rc are empty for ${task.id}," +
                        "set exit to ${task.exitStatus}"
            }
            task.stdout = outputFile
            task.stderr = errorFile
        }
        return completed
    }
}
