/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.cloud.CloudTransferOptions
import nextflow.executor.Executor
import nextflow.util.Duration

/**
 * Helper class wrapping float config options required for job execution
 */
@Slf4j
@ToString(includeNames = true, includePackage = false)
@EqualsAndHashCode
@CompileStatic
@Deprecated
class FloatOptions implements CloudTransferOptions {


    Session session

    Duration delayBetweenAttempts = DEFAULT_DELAY_BETWEEN_ATTEMPTS

    private Map<String,String> env = System.getenv()

    /* Only for testing purpose */
    protected FloatOptions() {}

    FloatOptions(Executor executor) {
        this(executor.session)
    }

    FloatOptions(Session session) {
        this.session = session
    }

    String getStorageClass() {
        session.config.navigate('float.s3.uploadStorageClass') as String
    }

    String getKmsKeyId() {
        session.config.navigate('float.s3.storageKmsKeyId') as String
    }

    int getMaxParallelTransfers() {
        session.config.navigate('float.maxParallelTransfers', MAX_TRANSFER) as int
    }

    int getMaxTransferAttempts() {
        String dft = env.AWS_MAX_ATTEMPTS ? env.AWS_MAX_ATTEMPTS as int : 5
        session.config.navigate('float.maxTransferAttempts', dft) as int
    }

    Duration getDelayBetweenAttempts() {
        Duration dft = DEFAULT_DELAY_BETWEEN_ATTEMPTS
        session.config.navigate('float.delayBetweenAttempts', dft) as Duration
    }

    String getAwsRegion() {
        session.config.navigate('float.region') as String
    }

    String getStorageEncryption() {
        session.config.navigate('float.storageEncryption') as String
    }

    String getNfs() {
        session.config.navigate('float.nfs') as String
    }

    String getStorageKmsKeyId() {
        return session.config.navigate('float.storageKmsKeyId') as String
    }

    String getAwsCli() {
        String res = session.config.navigate('float.awsCliPath') as String
        if (!res) {
            res = 'aws'
        }
        if (awsRegion) {
            res += " --region $awsRegion "
        }
        return res
    }

    String getRetryMode() {
        String ret = session.config.navigate('float.retryMode', 'standard')
        if (ret == 'built-in')
            ret = null // force falling back on NF built-in retry mode
        return ret
    }

    Boolean getDebug() {
        session.config.navigate('float.debug') as Boolean
    }
}
