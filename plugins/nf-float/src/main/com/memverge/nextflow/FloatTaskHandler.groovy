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

import java.nio.file.FileSystems
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.GridTaskHandler
import nextflow.processor.TaskRun

/**
 * Float task handler
 */
@Slf4j
@CompileStatic
class FloatTaskHandler extends GridTaskHandler {

    FloatTaskHandler( TaskRun task, FloatGridExecutor executor ) {
        super(task, executor)
    }

    /**
     * Override the grid task handler to never create the task directory.
     */
    @Override
    protected ProcessBuilder createProcessBuilder() {

        // -- log the submit command
        final cli = ((FloatGridExecutor)executor).getSubmitCommandLine(this, wrapperFile)
        log.trace "start process ${task.name} > cli: ${cli}"

        // -- prepare submit process
        final builder = new ProcessBuilder()
            .command( cli as String[] )
            .redirectErrorStream(true)

        if( task.workDir.fileSystem == FileSystems.default ) {
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

}
