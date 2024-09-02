package com.memverge.nextflow

import nextflow.executor.BashFunLib

class FloatBashLib extends BashFunLib<FloatBashLib> {
    static String script(FloatConf conf) {
        new FloatBashLib()
                .includeCoreFun(true)
                .withMaxParallelTransfers(conf.maxParallelTransfers)
                .render()
    }
}
