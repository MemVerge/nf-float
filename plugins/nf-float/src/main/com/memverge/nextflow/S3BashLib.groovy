/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import nextflow.Global
import nextflow.Session
import nextflow.executor.BashFunLib

@Deprecated
class S3BashLib extends BashFunLib<S3BashLib> {
    private String storageClass = 'STANDARD'
    private String storageEncryption = ''
    private String storageKmsKeyId = ''
    private String debug = ''
    private String cli = 'aws'
    private String retryMode

    S3BashLib withCliPath(String cliPath) {
        if (cliPath) {
            this.cli = cliPath
        }
        return this
    }

    S3BashLib withRetryMode(String value) {
        if (value) {
            retryMode = value
        }
        return this
    }

    S3BashLib withDebug(Boolean value) {
        this.debug = value ? '--debug' : ''
        return this
    }

    S3BashLib withStorageClass(String value) {
        if (value) {
            this.storageClass = value
        }
        return this
    }

    S3BashLib withStorageEncryption(String value) {
        if (value) {
            this.storageEncryption = value ? "--see $value " : ''
        }
        return this
    }

    S3BashLib withStorageKmsKeyId(String value) {
        if (value) {
            this.storageKmsKeyId = value ? "--sse-kms-key-id $value " : ''
        }
        return this
    }

    protected String retryEnv() {
        if (!retryMode) {
            return ''
        }
        """
        # aws cli retry config
        export AWS_RETRY_MODE=${retryMode}
        export AWS_MAX_ATTENMPTS=${maxTransferAttempts}
        """.stripIndent().rightTrim()
    }

    protected String s3Lib() {
        """
        # aws helper
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              $cli s3 cp --only-show-errors ${debug} ${storageEncryption} ${storageKmsKeyId} --storage-class $storageClass - "\$s3path"
            elif [[ -d "\$name" ]]; then
              $cli s3 cp --only-show-errors --recursive ${debug} ${storageEncryption} ${storageKmsKeyId} --storage-class $storageClass "\$name" "\$s3path/\$name"
            else
              $cli s3 cp --only-show-errors ${debug} ${storageEncryption} ${storageKmsKeyId} --storage-class $storageClass "\$name" "\$s3path/\$name"
            fi
        }
        
        nxf_s3_download() {
            local source=\$1
            local target=\$2
            local file_name=\$(basename \$1)
            local is_dir=\$($cli s3 ls \$source | grep -F "PRE \${file_name}/" -c)
            if [[ \$is_dir == 1 ]]; then
                $cli s3 cp --only-show-errors --recursive "\$source" "\$target"
            else 
                $cli s3 cp --only-show-errors "\$source" "\$target"
            fi
        }
        """.stripIndent()
    }

    String render() {
        super.render() + retryEnv() + s3Lib()
    }

    static private S3BashLib lib0(FloatOptions opts, boolean includeCore) {
        new S3BashLib()
                .includeCoreFun(includeCore)
                .withMaxParallelTransfers(opts.maxParallelTransfers)
                .withDelayBetweenAttempts(opts.delayBetweenAttempts)
                .withMaxTransferAttempts(opts.maxTransferAttempts)
                .withCliPath(opts.awsCli)
                .withStorageClass(opts.storageClass)
                .withStorageEncryption(opts.storageEncryption)
                .withStorageKmsKeyId(opts.storageKmsKeyId)
                .withRetryMode(opts.retryMode)
                .withDebug(opts.debug)
    }

    static String script(FloatOptions opts) {
        lib0(opts, true).render()
    }

    static String script() {
        final opts = new FloatOptions(Global.session as Session)
        lib0(opts, false).render()
    }
}
