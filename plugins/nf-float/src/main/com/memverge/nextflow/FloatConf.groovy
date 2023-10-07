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
import nextflow.exception.AbortOperationException
import nextflow.io.BucketParser
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
    static final String S3_SCHEMA = "s3"
    static final String ADDR_SEP = ","
    static final String NF_JOB_ID = "nf-job-id"
    static final String NF_PROCESS_NAME = 'nextflow-io-process-name'
    static final String NF_RUN_NAME = 'nextflow-io-run-name'
    static final String NF_SESSION_ID = 'nextflow-io-session-id'
    static final String NF_TASK_NAME = 'nextflow-io-task-name'
    static final String NF_INPUT_SIZE = 'input-size'

    /** credentials for op center */
    String username
    String password
    Collection<String> addresses
    String nfs

    String s3accessKey
    String s3secretKey

    /** parameters for submitting the tasks */
    String vmPolicy
    String migratePolicy
    String extraOptions
    String commonExtra

    float timeFactor = 1
    float cpuFactor = 1
    float memoryFactory = 1

    float maxCpuFactor = 2
    float maxMemoryFactor = 2

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

        ret.initAwsConf(config)
        ret.initFloatConf(config.float as Map)

        return ret
    }

    private static boolean isS3(URI input) {
        return input.getScheme() == S3_SCHEMA
    }

    String getInputVolume(URI input) {
        if (isS3(input)) {
            def options = ["mode=rw"]
            if (s3accessKey && s3secretKey) {
                options.add("accesskey=" + s3accessKey)
                options.add("secret=" + s3secretKey)
            }
            final optionsStr = options.join(",")

            // the s3 URI may contains 3 slashes, replace it with 2
            def string = input.toString().replaceAll("///", "//")
            final bucket = BucketParser.from(string).bucket
            return "[$optionsStr]$S3_SCHEMA://$bucket:/$bucket"
        }
        return ""
    }

    String getWorkDirVol(URI workDir) {
        if (isS3(workDir)) {
            return getInputVolume(workDir)
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

    private String parseNfs(String nfsOption) {
        def vol = new DataVolume(nfsOption)
        vol.setS3Credentials(s3accessKey, s3secretKey)
        return vol.toString()
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
        this.nfs = parseNfs(floatNode.nfs as String)

        if (floatNode.vmPolicy) {
            this.vmPolicy = collapseMapToString(floatNode.vmPolicy as Map)
        }
        if (floatNode.migratePolicy) {
            this.migratePolicy = collapseMapToString(floatNode.migratePolicy as Map)
        }
        if (floatNode.extraOptions) {
            this.extraOptions = floatNode.extraOptions as String
        }
        if (floatNode.timeFactor) {
            this.timeFactor = floatNode.timeFactor as Float
        }
        if (floatNode.cpuFactor) {
            this.cpuFactor = floatNode.cpuFactor as Float
        }
        if (floatNode.maxCpuFactor) {
            this.maxCpuFactor = floatNode.maxCpuFactor as Float
        }
        if (floatNode.maxMemoryFactor) {
            this.maxMemoryFactor = floatNode.maxMemoryFactor as Float
        }
        if (floatNode.memoryFactor) {
            this.memoryFactory = floatNode.memoryFactor as Float
        }
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

    private static String collapseMapToString(Map map) {
        final collapsedStr = map
                .toConfigObject()
                .flatten()
                .collect((k, v) -> "${k}=${v}")
                .join(',')
        return "[${collapsedStr}]"
    }

    private static void warnDeprecated(String deprecated, String replacement) {
        log.warn "[float] config option `$deprecated` " +
                "is no longer supported, " +
                "use `$replacement` instead"
    }

    private void initAwsConf(Map conf) {
        def cred = Global.getAwsCredentials(System.getenv(), conf)
        if (cred && cred.size() > 1) {
            s3accessKey = cred[0]
            s3secretKey = cred[1]
        }
    }

    void validate() {
        if (addresses.size() == 0) {
            throw new AbortOperationException("missing MMCE OC address")
        }
    }

    List<String> getCliPrefix(String address = "") {
        validate()
        if (StringUtils.length(address) == 0) {
            address = addresses[0]
        }
        def bin = FloatBin.get(address)
        List<String> ret = [
                bin.toString(),
                "-a",
                address]
        if (username && password) {
            ret.addAll(["-u",
                        username,
                        "-p",
                        password])
        }
        return ret
    }

    String getCli(String address = "") {
        return getCliPrefix(address).join(" ")
    }
}

class DataVolume {
    private Map<String, String> options
    private URI uri

    DataVolume(String s) {
        options = [:]
        if (!s) {
            uri = new URI("")
            return
        }
        def opStart = s.indexOf("[")
        def opEnd = s.indexOf("]")
        if (opStart == 0 && opEnd != -1) {
            def opStr = s.substring(1, opEnd)
            for (String op : opStr.split(",")) {
                def tokens = op.split("=")
                if (tokens.size() < 2) {
                    continue
                }
                options[tokens[0]] = tokens[1]
            }
            uri = new URI(s.substring(opEnd + 1))
        } else {
            uri = new URI(s)
        }
    }

    def setS3Credentials(String key, String secret) {
        if (scheme != "s3") {
            return
        }
        final accessKey = "accessKey"
        final secretKey = "secret"
        if (!options.containsKey(accessKey) || !options.containsKey(secretKey)) {
            if (key != null) {
                options[accessKey] = key
            }
            if (secret != null) {
                options[secretKey] = secret
            }
        }
        if (!options.containsKey('mode')) {
            options['mode'] = "rw"
        }
    }

    def getScheme() {
        return uri.scheme
    }

    String toString() {
        List<String> ops = []
        options.forEach { k, v -> ops.add("$k=$v") }
        if (ops.size() == 0) {
            return uri.toString()
        }
        Collections.sort(ops)
        return "[${ops.join(",")}]${uri}"
    }
}
