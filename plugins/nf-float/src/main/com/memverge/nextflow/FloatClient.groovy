/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import nextflow.executor.BashFunLib

/**
 * float command line helper
 */
class FloatClient extends BashFunLib<FloatClient> {
    private FloatConf conf

    static FloatClient create(FloatConf conf) {
        FloatClient ret = new FloatClient().includeCoreFun(true)
        ret.conf = conf
        return ret
    }

    private String killCmd(String taskID) {
        String cli = conf.getCli()
        return "${cli} scancel -j ${taskID}"
    }

    private CmdResult getResult(String cmd) {
        return CmdResult.of(cmd, conf.cmdTimeoutMS())
    }

    CmdResult submit(String script,
                     int cpu,
                     int memGB,
                     String image) {
        def cmd = "sbatch -i ${image} -j ${script} --cpu ${cpu} --mem ${memGB}"
        return getResult(cmd)
    }

    CmdResult query(String jobID) {
        return getResult("show -j ${jobID}")
    }

    CmdResult getStdout(String jobID) {
        return getResult("log cat -j ${jobID} stdout.autosave")
    }

    CmdResult getStderr(String jobID) {
        return getResult("log cat -j ${jobID} stderr.autosave")
    }

    CmdResult kill(String jobID) {
        return getResult("${killCmd(jobID)} scancel -j ${jobID}")
    }

    List<String> getCmdPrefix() {
        return ['float',
        '-a', conf.address,
        '-u', conf.username,
        '-p', conf.password]
    }
}
