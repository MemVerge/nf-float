/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.util.Escape

import java.nio.file.Path

/**
 * Defines the script operation to handle file
 */
@Slf4j
@CompileStatic
@Deprecated
class FloatAwsFileCopyStrategy extends SimpleFileCopyStrategy {
    private FloatOptions opts

    private Map<String, String> environment

    FloatAwsFileCopyStrategy(TaskBean task, FloatOptions opts) {
        super(task)
        this.opts = opts
        this.environment = environment
    }

    /**
     * @return A script snippet that download from S3 the task scripts:
     * {@code .command.env}, {@code .command.sh}, {@code .command.in},
     * etc.
     */
    String getBeforeStartScript() {
        S3BashLib.script(opts)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getEnvScript(Map environment, boolean container) {
        final result = new StringBuilder()
        final copy = environment ?
                new HashMap<String, String>(environment) :
                Collections.<String, String>emptyMap()
        final path = copy.containsKey('PATH')
        if (path) {
            copy.remove('PATH')
        }
        // finally render the environment
        final envSnippet = super.getEnvScript(copy, false)
        if (envSnippet) {
            result << envSnippet
        }
        return result.toString()
    }

    @Override
    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        def result = 'downloads=(true)\n'
        result += super.getStageInputFilesScript(inputFiles) + '\n'
        result += 'nxf_parallel "${downloads[@]}"\n'
        return result
    }

    @Override
    String stageInputFile(Path path, String targetName) {
        // third param should not be escaped, because it's used in the grep match rule
        def stage_cmd = opts.maxTransferAttempts > 1 && !opts.retryMode
                ? "downloads+=(\"nxf_cp_retry nxf_s3_download s3:/${Escape.path(path)} ${Escape.path(targetName)}\")"
                : "downloads+=(\"nxf_s3_download s3:/${Escape.path(path)} ${Escape.path(targetName)}\")"
        return stage_cmd
    }

    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {
        final patterns = normalizeGlobStarPaths(outputFiles)
        // create a bash script that will copy the out file to the working directory
        log.trace "[Float] Unstaging file path: $patterns"

        if( !patterns )
            return null

        final escape = new ArrayList(outputFiles.size())
        for( String it : patterns )
            escape.add( Escape.path(it) )

        return """\
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escape.join(' ')}" | sort | uniq); do
                uploads+=("nxf_s3_upload '\$name' s3:/${Escape.path(targetDir)}")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
            """.stripIndent(true)
    }

    @Override
    String touchFile(Path file) {
        "echo start | nxf_s3_upload - s3:/${Escape.path(file)}"
    }

    @Override
    String fileStr(Path path) {
        Escape.path(path.getFileName())
    }

    @Override
    String copyFile(String name, Path target) {
        "nxf_s3_upload ${Escape.path(name)} s3:/${Escape.path(target.getParent())}"
    }

    static String uploadCmd( String source, Path target ) {
        "nxf_s3_upload ${Escape.path(source)} s3:/${Escape.path(target)}"
    }

    String exitFile( Path path ) {
        "| nxf_s3_upload - s3:/${Escape.path(path)} || true"
    }

    @Override
    String pipeInputFile( Path path ) {
        " < ${Escape.path(path.getFileName())}"
    }
}
