/*
 * Copyright 2024, MemVerge Corporation
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

import java.nio.file.Paths

class FloatFileCopyStrategyTest extends BaseTest {

    def "get before script with conf"() {
        given:
        def conf = [
                float: [maxParallelTransfers: 10]
        ]

        when:
        def fConf = FloatConf.getConf(conf)
        def strategy = new FloatFileCopyStrategy(fConf)
        final script = strategy.beforeStartScript

        then:
        script.contains('cpus>10')
        !script.contains('\nnull')
    }

    def "get unstage output file script"() {
        given:
        def conf = [float:[]]

        when:
        def fConf = FloatConf.getConf(conf)
        def strategy = new FloatFileCopyStrategy(fConf)
        final script = strategy.getUnstageOutputFilesScript(
                ['a',], Paths.get('/target/A'))

        then:
        script.contains('eval "ls -1d a"')
        script.contains('uploads+=("nxf_fs_move "$name" /target/A")')
    }
}
