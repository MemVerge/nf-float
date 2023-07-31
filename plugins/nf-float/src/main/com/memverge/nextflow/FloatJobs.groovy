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

import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.processor.TaskId
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.StringUtils

import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

@Slf4j
class FloatJobs {
    static final String Completed = 'Completed'
    static final String Unknown = 'Unknown'

    private Map<String, String> job2status
    private Map<String, String> job2oc
    private Map<String, Path> task2workDir
    private Collection<String> ocs
    private String taskPrefix

    FloatJobs(Collection<String> ocAddresses) {
        if (!ocAddresses) {
            throw new IllegalArgumentException('op-center address is empty')
        }
        job2status = new ConcurrentHashMap<>()
        job2oc = new ConcurrentHashMap<>()
        task2workDir = new ConcurrentHashMap<>()
        ocs = ocAddresses
        def charset = (('a'..'z') + ('0'..'9')).join('')
        taskPrefix = RandomStringUtils.random(
                6, charset.toCharArray())
    }

    def setTaskPrefix(String prefix) {
        taskPrefix = prefix
    }

    String getJobName(TaskId id) {
        return "${taskPrefix}-${id}"
    }

    String getOc(String jobID) {
        return job2oc.getOrDefault(jobID, ocs[0])
    }

    Map<String, String> getJob2Status() {
        return job2status
    }

    def setWorkDir(TaskId taskID, Path dir) {
        def name = getJobName(taskID)
        task2workDir[name] = dir
    }

    Map<String, String> parseQStatus(String text) {
        return updateOcStatus(ocs[0], text)
    }

    def updateOcStatus(String oc, String text) {
        def stMap = CmdResult.with(text).getQStatus()
        stMap.each { key, value ->
            def jobID = key as String
            def st = value.status
            def taskID = value.taskID
            if (!jobID || !st) {
                return
            }
            job2oc.put(jobID, oc)

            def currentSt = job2status.get(jobID, Unknown)
            def workDir = task2workDir.get(taskID)
            if (workDir) {
                // check the availability of result files
                // call list files to update the folder cache
                FileHelper.listDirectory(workDir)
                def files = ['.command.out', '.command.err', '.exitcode']
                if (currentSt != Completed && st == Completed) {
                    for (filename in files) {
                        def name = workDir.resolve(filename)
                        try {
                            !FileHelper.checkIfExists(name, [checkIfExists: true])
                        } catch (NoSuchFileException ex) {
                            log.info("[float] job $jobID completed " +
                                    "but file not found: $ex")
                            return
                        }
                    }
                    log.debug("[float] found $files in: $workDir")
                }
            }
            job2status.put(jobID, st)
        }
        log.debug "[float] update op-center $oc job status: $job2status"
        return job2status
    }

    def updateStatus(Map<String, List<String>> cmdMap) {
        cmdMap.entrySet().parallelStream().map { entry ->
            def oc = entry.key
            def cmd = entry.value
            log.debug "[float] getting queue status > cmd: ${cmd.join(' ')}"
            try {
                final buf = new StringBuilder()
                final process = new ProcessBuilder(cmd).redirectErrorStream(true).start()
                final consumer = process.consumeProcessOutputStream(buf)
                process.waitForOrKill(60_000)
                final exit = process.exitValue(); consumer.join() // <-- make sure sync with the output consume #1045
                final result = buf.toString()

                if (exit == 0) {
                    log.trace "[float] queue status on $oc > cmd exit: $exit\n$result"
                    updateOcStatus(oc, result)
                } else {
                    def m = """\
                [float] queue status on $oc cannot be fetched
                - cmd executed: ${cmd.join(' ')}
                - exit status : $exit
                - output      :
                """.stripIndent()
                    m += result.indent('  ')
                    log.warn1(m, firstOnly: true)
                }
            } catch (Exception e) {
                log.warn "[float] failed to retrieve queue status -- See the log file for details", e
            }
        }.collect()
        log.debug "[float] collecting job status completes."
    }
}
