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

import groovy.transform.WithReadLock
import groovy.transform.WithWriteLock
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.processor.TaskId
import org.apache.commons.lang.RandomStringUtils

import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

@Slf4j
class FloatJobs {
    static final String Completed = 'Completed'
    static final String Unknown = 'Unknown'

    private Map<String, FloatJob> nfJobID2FloatJob
    private Map<String, String> floatJobID2oc
    private Map<String, Path> nfJobID2workDir
    private Collection<String> ocs
    private String taskPrefix

    FloatJobs(Collection<String> ocAddresses) {
        if (!ocAddresses) {
            throw new IllegalArgumentException('op-center address is empty')
        }
        nfJobID2FloatJob = new ConcurrentHashMap<>()
        floatJobID2oc = new ConcurrentHashMap<>()
        nfJobID2workDir = new ConcurrentHashMap<>()
        ocs = ocAddresses
        def charset = (('a'..'z') + ('0'..'9')).join('')
        taskPrefix = RandomStringUtils.random(
                6, charset.toCharArray())
    }

    def setTaskPrefix(String prefix) {
        taskPrefix = prefix
    }

    String getNfJobID(TaskId id) {
        return "${taskPrefix}-${id}"
    }

    String getOc(String floatJobID) {
        return floatJobID2oc.getOrDefault(floatJobID, ocs[0])
    }

    @WithReadLock
    Map<String, FloatJob> getNfJobID2job() {
        return nfJobID2FloatJob
    }

    def setWorkDir(TaskId taskID, Path dir) {
        def name = getNfJobID(taskID)
        nfJobID2workDir[name] = dir
    }

    Map<String, FloatJob> parseQStatus(String text) {
        return updateOcStatus(ocs[0], text)
    }

    String getJobStatus(String nfJobID) {
        FloatJob job = nfJobID2FloatJob.get(nfJobID)
        if (job == null) {
            return Unknown
        }
        return job.status
    }

    def refreshWorkDir(String nfJobID) {
        def workDir = nfJobID2workDir.get(nfJobID)
        if (workDir) {
            // call list files to update the folder cache
            FileHelper.listDirectory(workDir)
        }
    }

    @WithWriteLock
    def updateOcStatus(String oc, String text) {
        def stMap = FloatJob.parseJobMap(text)
        stMap.each { nfJobID, job ->
            if (!job.nfJobID || !job.status) {
                return
            }
            floatJobID2oc.put(job.floatJobID, oc)
            def currentSt = getJobStatus(nfJobID)
            def workDir = nfJobID2workDir.get(job.nfJobID)
            if (workDir) {
                // check the availability of result files
                refreshWorkDir(job.nfJobID)
                def files = ['.command.out', '.command.err', '.exitcode']
                if (currentSt != Completed && job.status == Completed) {
                    for (filename in files) {
                        def name = workDir.resolve(filename)
                        try {
                            !FileHelper.checkIfExists(name, [checkIfExists: true])
                        } catch (NoSuchFileException ex) {
                            log.info "[float] job $nfJobID completed " +
                                    "but file not found: ${ex.message}"
                            job.status = currentSt
                            return
                        }
                    }
                    log.debug "[float] found $files in: $workDir"
                }
            }

        }
        nfJobID2FloatJob += stMap
        log.debug "[float] update op-center $oc job status"
        return nfJobID2FloatJob
    }

    def updateStatus(Map<String, List<String>> cmdMap) {
        cmdMap.entrySet().parallelStream().map { entry ->
            def oc = entry.key
            def cmd = entry.value
            log.debug "[float] getting queue status > cmd: ${cmd.join(' ')}"
            try {
                final res = Global.execute(cmd)

                if (res.succeeded) {
                    log.trace "[float] queue status on $oc > cmd exit: $exit\n$result"
                    updateOcStatus(oc, res.out)
                } else {
                    def m = """\
                [float] queue status on $oc cannot be fetched
                - cmd executed: ${cmd.join(' ')}
                - exit status : ${res.exit}
                - output      :
                """.stripIndent()
                    m += res.out.indent('  ')
                    log.warn1(m, firstOnly: true)
                }
            } catch (Exception e) {
                log.warn "[float] failed to retrieve queue status -- See the log file for details", e
            }
        }.collect()
        log.debug "[float] collecting job status completes."
    }
}
