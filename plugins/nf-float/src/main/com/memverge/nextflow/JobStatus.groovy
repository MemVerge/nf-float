package com.memverge.nextflow

class JobStatus {
    String taskID
    String status

    JobStatus(String taskID, String status) {
        this.taskID = taskID
        this.status = status
    }
}
