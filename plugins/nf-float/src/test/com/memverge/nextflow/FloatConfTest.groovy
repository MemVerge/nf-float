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

class FloatConfTest extends BaseTest {
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

    def "credentials are required"() {
        given:
        def conf = [
                float: [address: '1.2.3.4']]

        when:
        def fConf = FloatConf.getConf(conf)
        fConf.validate()

        then:
        thrown AbortOperationException
    }

    def "get s3 data volume"() {
        given:
        def conf = [
                aws: [accessKey: 'ak0',
                      secretKey: 'sk0']]

        when:
        def fConf = FloatConf.getConf(conf)
        def workDir = new URI('s3://bucket/work/dir')
        def volume = fConf.getWorkDirVol(workDir)

        then:
        volume == '[mode=rw,accesskey=ak0,secret=sk0]' +
                's3://bucket:/bucket'
    }

    def "get s3 credentials from env 0"() {
        given:
        setEnv('AWS_ACCESS_KEY_ID', 'aak_id')
        setEnv('AWS_SECRET_ACCESS_KEY', 'asa_key')

        when:
        def fConf = FloatConf.getConf()
        def workDir = new URI('s3:///bucket/work/dir')
        def volume = fConf.getWorkDirVol(workDir)

        then:
        volume == '[mode=rw,accesskey=aak_id,secret=asa_key]' +
                's3://bucket:/bucket'

        cleanup:
        setEnv('AWS_ACCESS_KEY_ID', '')
        setEnv('AWS_SECRET_ACCESS_KEY', '')
    }

    def "get s3 credentials from env 1" () {
        given:
        setEnv('AWS_ACCESS_KEY', 'aak')
        setEnv('AWS_SECRET_KEY', 'ask')

        when:
        def fConf = FloatConf.getConf()
        def workDir = new URI('s3://bucket/work/dir')
        def volume = fConf.getWorkDirVol(workDir)

        then:
        volume == '[mode=rw,accesskey=aak,secret=ask]' +
                's3://bucket:/bucket'

        cleanup:
        setEnv('AWS_ACCESS_KEY', '')
        setEnv('AWS_SECRET_KEY', '')
    }

    def "get local path with nfs"() {
        given:
        def conf = [float: [nfs: 'nfs://1.2.3.4/work/dir']]

        when:
        def fConf = FloatConf.getConf(conf)
        def workDir1 = new URI('file:///my/work/dir')
        def volume1 = fConf.getWorkDirVol(workDir1)

        def workDir2 = new URI('/my/work/dir')
        def volume2 = fConf.getWorkDirVol(workDir2)

        final expected = 'nfs://1.2.3.4/work/dir:/my/work/dir'

        then:
        volume1 == expected
        volume2 == expected
    }

    def "get local path with nfs with mount point"() {
        given:
        def fConf = FloatConf.getConf(
                [float: [nfs: 'nfs://1.2.3.4/work/dir:/local']])

        when:
        def workDir = new URI('file:///local/here')
        def volume = fConf.getWorkDirVol(workDir)

        then:
        volume == 'nfs://1.2.3.4/work/dir:/local'
    }
}
