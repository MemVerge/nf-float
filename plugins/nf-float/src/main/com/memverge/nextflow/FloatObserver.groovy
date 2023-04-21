/*
 * Copyright 2022, MemVerge Corp.
 */

package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

/**
 * plugin pipeline events observer
 */
@Slf4j
@CompileStatic
class FloatObserver implements TraceObserver {

    @Override
    void onFlowCreate(Session session) {
        log.info "starting the pipeline on float"
    }

    @Override
    void onFlowComplete() {
        log.info "complete the pipeline on float"
    }
}
