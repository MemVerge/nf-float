/*
 * Copyright 2022-2023, MemVerge Corporation
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

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import java.util.regex.Matcher

/**
 * CmdResult keeps the output, error of a command
 */
@Slf4j
class CmdResult {
    String out
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

    Map<String, JobStatus> getQStatus() {
        Map<String, JobStatus> ret = new HashMap<>()
        try {
            def parser = new JsonSlurper()
            def obj = parser.parseText(out)
            for (i in obj) {
                def id = i.id as String
                def status = i.status as String
                def tags = i.customTags as Map
                String taskID = tags ? tags[FloatConf.NF_JOB_ID] : ""
                if (id && status && taskID) {
                    ret[id] = new JobStatus(taskID, status)
                }
            }
        } catch (Exception e) {
            log.warn "failed to parse: ${out}, detail: ${e.toString()}"
        }
        return ret
    }
}
