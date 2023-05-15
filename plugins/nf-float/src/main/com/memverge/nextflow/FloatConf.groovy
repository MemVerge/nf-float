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

        if (config && config.float instanceof Map) {
            Map node = ((Map) config.float)
            ret.username = node.username
            ret.password = node.password
            if (node.address instanceof Collection) {
                ret.addresses = (node.address as Collection).collect {
                    it.toString()
                }
            } else {
                ret.addresses = node.address.toString()
                        .split(",")
                        .toList()
                        .stream()
                        .filter { it.size() > 0 }
                        .map { it.trim() }
                        .collect()
            }
            ret.commonExtra = node.commonExtra
            ret.nfs = node.nfs
            if (node.cpu) {
                ret.cpu = node.cpu as String
            }
            if (node.cpus) {
                ret.cpu = node.cpus as String
            }
            if (node.mem) {
                def unit = "${node.mem as String} GB"
                ret.memGB = MemoryUnit.of(unit)
            }
            if (node.memory) {
                ret.memGB = MemoryUnit.of(node.memory as String)
            }
            if (node.image) {
                ret.image = node.image as String
            }
            if (node.container) {
                ret.image = node.container as String
            }
        }
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
