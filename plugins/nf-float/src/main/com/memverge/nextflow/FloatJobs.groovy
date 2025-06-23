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

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

@Slf4j
class FloatJobs {
    private Map<String, FloatJob> nfJobID2FloatJob
    private Map<String, Path> nfJobID2workDir
    private String taskPrefix

    FloatJobs(String runName) {
        nfJobID2FloatJob = new ConcurrentHashMap<>()
        nfJobID2workDir = new ConcurrentHashMap<>()
        taskPrefix = runName
        log.info "[FLOAT] FloatJobs created with task prefix ${taskPrefix}"
    }

    def setTaskPrefix(String prefix) {
        taskPrefix = prefix
    }

    String getNfJobID(TaskId id) {
        return "${taskPrefix}-${id}"
    }

    TaskId getTaskID(String floatJobID) {
        // go through all float jobs to find the task id
        for (FloatJob job : nfJobID2FloatJob.values()) {
            if (job.floatJobID == floatJobID) {
                // extract the task id from the nfJobID
                def taskId = job.nfJobID.substring(taskPrefix.length() + 1)
                return new TaskId(Integer.parseInt(taskId))
            }
        }
        return null
    }

    FloatJob getJobByJobID(String jobID) {
        return nfJobID2FloatJob.get(jobID)
    }

    FloatJob getJob(TaskId id) {
        def nfJobID = getNfJobID(id)
        return getJobByJobID(nfJobID)
    }

    def setWorkDir(TaskId id, Path dir) {
        def name = getNfJobID(id)
        nfJobID2workDir[name] = dir
    }

    FloatJob updateJob(FloatJob job) {
        if (job.nfJobID == null || job.nfJobID.size() == 0) {
            log.error "[FLOAT] job nf Job ID is null or empty for job ${job.floatJobID}"
        } else {
            FloatJob existingJob = nfJobID2FloatJob.get(job.nfJobID)
            if (existingJob != null && existingJob.doneOrFailed) {
                log.info "[FLOAT] job ${job.nfJobID} already finished, no need to update"
                job = existingJob
            } else {
                if (existingJob == null) {
                    log.info "[FLOAT] put job ${job.nfJobID} in status tracking map"
                }
                nfJobID2FloatJob.put(job.nfJobID, job)
            }
            if (job.doneOrFailed) {
                refreshWorkDir(job.nfJobID)
            }
        }
        return job
    }

    Map<String, FloatJob> getNfJobID2Job() {
        return nfJobID2FloatJob
    }

    def refreshWorkDir(String nfJobID) {
        def workDir = nfJobID2workDir.get(nfJobID)
        if (workDir) {
            // call list files to update the folder cache
            FileHelper.listDirectory(workDir)
        }
    }
}
