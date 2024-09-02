/*
 * Copyright 2024, MemVerge Corporation
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
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.file.FileSystemPathFactory
import nextflow.util.Escape

import java.nio.file.Path


@Slf4j
@CompileStatic
class FloatFileCopyStrategy extends SimpleFileCopyStrategy {
    private FloatConf conf

    FloatFileCopyStrategy(FloatConf conf) {
        super()
        this.conf = conf
    }

    @Override
    String getBeforeStartScript() {
        def script = FloatBashLib.script(conf)
        final lib = FileSystemPathFactory.bashLib(targetDir)
        if (lib) {
            script += '\n' + lib
        }
        return script.leftTrim()
    }

    /**
     * Creates the script to unstage the result output files from the scratch directory
     * to the shared working directory
     */
    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {
        final patterns = normalizeGlobStarPaths(outputFiles)
        // create a bash script that will copy the out file to the working directory
        log.info "Unstaging file path: $patterns, outputFiles: $outputFiles targetDir: $targetDir"

        if (!patterns)
            return null

        final escape = new ArrayList(outputFiles.size())
        for (String it : patterns)
            escape.add(Escape.path(it))

        final mode = stageoutMode ?: (workDir == targetDir ? 'copy' : 'move')
        return """\
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escape.join(' ')}" | sort | uniq); do
                uploads+=("${stageOutCommand('$name', targetDir, mode)}")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
            """.stripIndent(true)
    }
}
