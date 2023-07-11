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

import nextflow.exception.AbortOperationException
import spock.lang.Specification

class FloatConfTest extends Specification {
    def "one op-center in the address"() {
        given:
        def conf = [
                float: [address: "1.2.3.4"]
        ]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.addresses == ["1.2.3.4"]
    }

    def "2 op-centers in the address"() {
        given:
        def conf = [
                float: [address: "1.2.3.4,2.3.4.5"]
        ]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.addresses == ["1.2.3.4", "2.3.4.5"]
    }

    def "2 op-centers in the address"() {
        given:
        def conf = [
                float: [address: ["1.2.3.4", "2.3.4.5"]]
        ]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.addresses == ["1.2.3.4", "2.3.4.5"]
    }

    def "get cli without address"() {
        given:
        def conf = [
                float: [address : "1.2.3.4, 2.3.4.5",
                        username: "admin",
                        password: "password"]]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.getCli() == "float -a 1.2.3.4 " +
                "-u admin -p password"

    }

    def "get cli with address"() {
        given:
        def conf = [
                float: [address : "1.2.3.4, 2.3.4.5",
                        username: "admin",
                        password: "password"]]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.getCli("1.1.1.1") == "float -a 1.1.1.1 " +
                "-u admin -p password"
    }

    def "get memory from mem" () {
        given:
        def conf = [
                float: [mem: 5]]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.memGB.toGiga() == 5
    }

    def "get memory from memory" () {
        given:
        def conf = [
                float: [memory: '3 GB']]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.memGB.toGiga() == 3
    }

    def "get default memory" () {
        given:
        def conf = [
                float: []]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.memGB.toGiga() == 4
    }

    def "get cpu from cpu" () {
        given:
        def conf = [
                float: [cpu: 5]]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.cpu == '5'
    }

    def "get cpu from cpus" () {
        given:
        def conf = [
                float: [cpus: 7]]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.cpu == '7'
    }

    def "get default cpu" () {
        given:
        def conf = [
                float: []]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.cpu == '2'
    }

    def "get image from image" () {
        given:
        def conf = [
                float: [image: 'a']]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.image == 'a'
    }

    def "get image from container" () {
        given:
        def conf = [
                float: [container: 'b']]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.image == 'b'
    }

    def "get config from process" () {
        given:
        def conf = [
                process: [cpus: 4,
                          memory: '5 GB',
                          container: 'busybox']]

        when:
        def fConf = FloatConf.getConf(conf)

        then:
        fConf.cpu == '4'
        fConf.memGB.toGiga() == 5
        fConf.image == 'busybox'
    }

    def "credentials are required" () {
        given:
        def conf = [
                float: [address: '1.2.3.4']]

        when:
        def fConf = FloatConf.getConf(conf)
        fConf.validate()

        then:
        thrown AbortOperationException
    }
}
