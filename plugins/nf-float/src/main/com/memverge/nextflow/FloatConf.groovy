/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
import org.apache.commons.lang.StringUtils

/**
 * @author Cedric Zhuang <cedric.zhuang@memverge.com>
 */
@CompileStatic
class FloatConf {
    /** credentials for op center */
    String username
    String password
    String address
    String nfs

    /** parameters for submitting the tasks */
    String cpu = '2'
    String memGB = '4'
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
            ret.address = node.address
            ret.commonExtra = node.commonExtra
            ret.nfs = node.nfs
            if (node.cpu) {
                ret.cpu = node.cpu as String
            }
            if (node.mem) {
                ret.memGB = node.mem as String
            }
            if (node.image) {
                ret.image = node.image as String
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
        if (!address) {
            throw new AbortOperationException("missing MMCE OC address")
        }
    }

    String getCli() {
        List<String> ret = [
                "float",
                "--address",
                address,
                "--username",
                username,
                "--password",
                password
        ]
        return StringUtils.join(ret, " ")
    }

    def cmdTimeoutMS() {cmdTimeoutSeconds * 1000}
}
