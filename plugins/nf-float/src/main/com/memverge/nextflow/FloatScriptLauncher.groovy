/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileStatic
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

/**
 * Implements BASH launcher script for Float jobs
 */
@CompileStatic
@Deprecated
class FloatScriptLauncher extends BashWrapperBuilder {
    FloatScriptLauncher(TaskBean bean, FloatOptions opts) {
        super(bean, new FloatAwsFileCopyStrategy(bean, opts))
        // enable the copying of output file to the S3 work dir
        if (scratch == null)
            scratch = true
        // include task script as an input to force its staging in the container work directory
        bean.inputFiles[TaskRun.CMD_SCRIPT] = bean.workDir.resolve(TaskRun.CMD_SCRIPT)
        // add the wrapper file when stats are enabled
        // NOTE: this must match the logic that uses the run script in BashWrapperBuilder
        if (isTraceRequired()) {
            bean.inputFiles[TaskRun.CMD_RUN] = bean.workDir.resolve(TaskRun.CMD_RUN)
        }
        // include task stdin file
        if (bean.input != null) {
            bean.inputFiles[TaskRun.CMD_INFILE] = bean.workDir.resolve(TaskRun.CMD_INFILE)
        }
    }

    @Override
    protected boolean fixOwnership() {
        return containerConfig?.fixOwnership
    }
}
