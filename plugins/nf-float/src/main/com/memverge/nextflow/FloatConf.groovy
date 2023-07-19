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
import nextflow.exception.AbortOperationException
import nextflow.util.MemoryUnit
import org.apache.commons.lang.StringUtils

/**
 * @author Cedric Zhuang <cedric.zhuang@memverge.com>
 */
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
    String cpu = '2'
    MemoryUnit memGB = MemoryUnit.of('4 GB')
    String image = 'cactus'
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

        if (config) {
            if (config.float instanceof Map) {
                ret.parseNode(config.float)
            }
            if (config.process instanceof Map) {
                ret.parseNode(config.process)
            }
        }
        ret.checkEnv()
        return ret
    }

    def checkEnv() {
        if (!username) {
            username = System.getenv(MMC_USERNAME)
        }
        if (!password) {
            password = System.getenv(MMC_PASSWORD)
        }
        if (!addresses) {
            addresses = parseAddr(System.getenv(MMC_ADDRESS))
        }
    }

    private static Collection<String> parseAddr(Object address) {
        return address.toString()
                .split(ADDR_SEP)
                .toList()
                .stream()
                .filter { it.size() > 0 }
                .map { it.trim() }
                .collect()
    }

    void parseNode(Object obj) {
        Map node = obj as Map
        if (node == null) {
            return
        }
        username = node.username ? node.username : username
        password = node.password ? node.password : password
        if (node.address) {
            if (node.address instanceof Collection) {
                addresses = node.address.collect { it.toString() }
            } else {
                addresses = parseAddr(node.address)
            }
        }
        commonExtra = node.commonExtra ? node.commonExtra : commonExtra
        nfs = node.nfs ? node.nfs : nfs
        cpu = node.cpu ? node.cpu : cpu
        cpu = node.cpus ? node.cpus : cpu
        if (node.mem) {
            def unit = "${node.mem as String} GB"
            memGB = MemoryUnit.of(unit)
        }
        memGB = node.memory ? MemoryUnit.of(node.memory as String) : memGB
        image = node.image ? node.image : image
        image = node.container ? node.container : image
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
