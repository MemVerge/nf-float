/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.executor.AbstractGridExecutor

import java.util.regex.Matcher

/**
 * CmdResult keeps the output, error of a command
 */
@Slf4j
class CmdResult {
    String out
    String err
    int rc

    static CmdResult with(String stdout) {
        return new CmdResult().tap {
            out = stdout
        }
    }

    static CmdResult of(String cmd, long timeoutMS) {
        log.debug("[float] execute command: ${cmd}")
        def proc = cmd.execute()
        def stdout = new StringBuilder(), stderr = new StringBuilder()
        proc.consumeProcessOutput(stdout, stderr)
        proc.waitForOrKill(timeoutMS)
        return new CmdResult().tap {
            out = stdout.toString()
            rc = proc.exitValue()
        }
    }

    String jobID() {
        def matcher = out =~ /(?ms)id: ([0-9a-zA-Z]+).*/
        return getMatch(matcher)
    }

    def succeeded() {
        return rc == 0
    }

    static String getMatch(Matcher matcher) {
        if (matcher.size() == 1) {
            def match = matcher[0]
            if (match.size() > 1) {
                return match[1]
            }
        }
        return ""
    }

    def status() {
        def matcher = out =~ /(?ms)status: ([0-9a-zA-Z]+).*/
        return getMatch(matcher)
    }

    boolean isJobRunning() {
        def st = status().toLowerCase()
        return st == "executing" ||
                st == "initializing" ||
                st == "submitted" ||
                st == "floating"
    }

    def jobRC() {
        def matcher = out =~ /(?ms)rc: (["0-9]+).*/
        def rc = getMatch(matcher)
        return rc.strip('"')
    }

    Map getQStatus() {
        def parser = new JsonSlurper()
        def obj = parser.parseText(out)

        def ret = [:]
        for (i in obj) {
            def id = i.id as String
            def status = i.status as String
            if (id && status) {
                ret[id] = status
            }
        }
        return ret
    }
}
