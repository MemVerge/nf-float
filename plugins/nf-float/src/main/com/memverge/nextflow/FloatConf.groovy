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
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.util.MemoryUnit
import org.apache.commons.lang.StringUtils

/**
 * @author Cedric Zhuang <cedric.zhuang@memverge.com>
 */
@Slf4j
@CompileStatic
class FloatConf {
    static String MMC_ADDRESS = "MMC_ADDRESS"
    static String MMC_USERNAME = "MMC_USERNAME"
    static String MMC_PASSWORD = "MMC_PASSWORD"
    static String ADDR_SEP = ","
    static String NF_JOB_ID = "nf-job-id"

    /** credentials for op center */
    String username
    String password
    Collection<String> addresses
    String nfs

    /** parameters for submitting the tasks */
    String commonExtra

    /** some extra default parameters */
    int cmdTimeoutSeconds = 30

    /**
     * Create a FloatConf instance and initialize the content from the
     * configuration.  The configuration should contain a "float" node.
     * This node contains the configurations of float.
     * @param config
     * @return
     */
    static FloatConf getConf(Map config) {
        FloatConf ret = new FloatConf()

        if (!config || config.float !instanceof Map)
            return ret

        Map node = config.float as Map
        ret.username = node.username ?: System.getenv(MMC_USERNAME)
        ret.password = node.password ?: System.getenv(MMC_PASSWORD)
        if (node.address instanceof Collection) {
            ret.addresses = node.address.collect { it.toString() }
        } else {
            String address = node.address ?: System.getenv(MMC_ADDRESS) ?: ""
            ret.addresses = address
                    .tokenize(ADDR_SEP)
                    .collect { it.trim() }
                    .findAll { it.size() > 0 }
        }
        ret.nfs = node.nfs
        ret.commonExtra = node.commonExtra

        if (node.cpu)
            log.warn "Config option `float.cpu` is no longer supported, use `process.cpus` instead"
        if (node.cpus)
            log.warn "Config option `float.cpus` is no longer supported, use `process.cpus` instead"
        if (node.mem)
            log.warn "Config option `float.mem` is no longer supported, use `process.memory` instead"
        if (node.memory)
            log.warn "Config option `float.memory` is no longer supported, use `process.memory` instead"
        if (node.image)
            log.warn "Config option `float.image` is no longer supported, use `process.container` instead"
        if (node.container)
            log.warn "Config option `float.container` is no longer supported, use `process.container` instead"

        return ret
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

    def cmdTimeoutMS() { cmdTimeoutSeconds * 1000 }
}
