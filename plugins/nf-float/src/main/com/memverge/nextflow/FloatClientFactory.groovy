/*
 * Copyright 2022, MemVerge Corporation
 */
package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@Slf4j
@CompileStatic
class FloatClientFactory {
    /**
     * The username used for MMCE Operation Center
     */
    private FloatConf floatConf

    /**
     * Reference to {@link FloatClient} object
     */
    private FloatClient floatClient

    String getUsername() { username }

    String getPassword() { password }

    String getAddress() { address }

    /**
     * Initialize the Float driver with default (empty) parameters
     */
    FloatClientFactory() {
        this(Collections.emptyMap())
    }

    /**
     * Initialize the Float driver with the specified parameters
     *
     * @param config
     *      A map holding the driver parameters:
     *      - username: the username of the user in ops center
     *      - password: the password of the user
     *      - address: the address of the ops center
     */
    FloatClientFactory(Map config) {
        floatConf = FloatConf.getConf(config)
        floatConf.validate()
    }

    /**
     * Gets or lazily creates a {@link FloatClient} instance given the
     * current configuration parameter
     * @return
     */
    synchronized FloatClient getFloatClient() {
        if (floatClient) {
            return floatClient
        }

        floatClient = FloatClient.create(floatConf)
        return floatClient
    }
}
