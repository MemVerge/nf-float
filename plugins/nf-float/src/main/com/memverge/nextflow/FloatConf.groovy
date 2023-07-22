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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.exception.AbortOperationException
import org.apache.commons.lang.StringUtils

/**
 * @author Cedric Zhuang <cedric.zhuang@memverge.com>
 */
@Slf4j
@CompileStatic
class FloatConf {
    static final String MMC_ADDRESS = "MMC_ADDRESS"
    static final String MMC_USERNAME = "MMC_USERNAME"
    static final String MMC_PASSWORD = "MMC_PASSWORD"
    static final String ADDR_SEP = ","
    static final String NF_JOB_ID = "nf-job-id"

    /** credentials for op center */
    String username
    String password
    Collection<String> addresses
    String nfs

    String s3accessKey
    String s3secretKey

    /** parameters for submitting the tasks */
    String commonExtra

    /**
     * Create a FloatConf instance and initialize the content from the
     * configuration.  The configuration should contain a "float" node.
     * This node contains the configurations of float.
     * @param config
     * @return
     */
    static FloatConf getConf(Map config = null) {
        if (config == null) {
            config = [:]
        }
        FloatConf ret = new FloatConf()

        ret.initFloatConf(config.float as Map)
        ret.initAwsConf(config)

        return ret
    }

    String getDataVolume(URI workDir) {
        final scheme = workDir.getScheme()
        if (scheme == "s3") {
            def options = ["mode=rw"]
            if (s3accessKey && s3secretKey) {
                options.add("accesskey=" + s3accessKey)
                options.add("secret=" + s3secretKey)
            }
            final optionsStr = options.join(",")
            final path = workDir.host ?
                    "/${workDir.host}${workDir.path}" :
                    workDir.path
            return "[$optionsStr]s3:/$path:$path"
        }
        // local directory, need nfs support
        if (!nfs) {
            log.warn "local work directory need nfs support"
            return ""
        }
        if (nfs.split(":").size() > 2) {
            // already have mount point
            return nfs
        }
        return "$nfs:${workDir.path}"
    }

    private def initFloatConf(Map floatNode) {
        if (!floatNode) {
            return
        }
        this.username = floatNode.username ?: System.getenv(MMC_USERNAME)
        this.password = floatNode.password ?: System.getenv(MMC_PASSWORD)
        if (floatNode.address instanceof Collection) {
            this.addresses = floatNode.address.collect { it.toString() }
        } else {
            String address = floatNode.address ?: System.getenv(MMC_ADDRESS) ?: ""
            this.addresses = address
                    .tokenize(ADDR_SEP)
                    .collect { it.trim() }
                    .findAll { it.size() > 0 }
        }
        this.nfs = floatNode.nfs
        this.commonExtra = floatNode.commonExtra

        if (floatNode.cpu)
            warnDeprecated("float.cpu", "process.cpus")
        if (floatNode.cpus)
            warnDeprecated("float.cpus", "process.cpus")
        if (floatNode.mem)
            warnDeprecated("float.mem", "process.memory")
        if (floatNode.memory)
            warnDeprecated("float.memory", "process.memory")
        if (floatNode.image)
            warnDeprecated("float.image", "process.container")
        if (floatNode.container)
            warnDeprecated("float.container", "process.container")
    }

    private static def warnDeprecated(String deprecated, String replacement) {
        log.warn "[flaot] config option `$deprecated` " +
                "is no longer supported, " +
                "use `$replacement` instead"
    }

    private def initAwsConf(Map conf) {
        def cred = Global.getAwsCredentials(System.getenv(), conf)
        if (cred && cred.size() > 1) {
            s3accessKey = cred[0]
            s3secretKey = cred[1]
        }
    }

    void validate() {
        if (!username) {
            throw new AbortOperationException("missing MMCE username")
        }
        if (!password) {
            throw new AbortOperationException("missing MMCE password")
        }
        if (addresses.size() == 0) {
            throw new AbortOperationException("missing MMCE OC address")
        }
    }

    List<String> getCliPrefix(String address = "") {
        validate()
        if (StringUtils.length(address) == 0) {
            address = addresses[0]
        }
        List<String> ret = [
                "float",
                "-a",
                address,
                "-u",
                username,
                "-p",
                password
        ]
        return ret
    }

    String getCli(String address = "") {
        return getCliPrefix(address).join(" ")
    }
}
