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


import java.nio.file.Paths

class FloatGridExecutorMultiOCTest extends FloatBaseTest {
    def addr = 'fa, fb,'

    @Override
    def newTestExecutor(Map config = null) {
        if (config == null) {
            config = [float: [address : addr,
                              username: user,
                              password: pass,
                              nfs     : nfs]]
        }
        return super.newTestExecutor(config)
    }

    def "submit job with round robin"() {
        given:
        def exec = newTestExecutor()

        when:
        def cmd1 = exec.getSubmitCommandLine(
                newTask(exec, 1), Paths.get(script))
        def cmd2 = exec.getSubmitCommandLine(
                newTask(exec, 2), Paths.get(script))
        def expected1 = submitCmd(addr: "fb", taskID: 1)
        def expected2 = submitCmd(addr: "fa", taskID: 2)

        then:
        cmd1.join(' ') == expected1.join(' ')
        cmd2.join(' ') == expected2.join(' ')
    }

    def "input multiple addresses as list"() {
        given:
        def exec = newTestExecutor()

        when:
        def cmd1 = exec.getSubmitCommandLine(
                newTask(exec, 2), Paths.get(script))
        def cmd2 = exec.getSubmitCommandLine(
                newTask(exec, 3), Paths.get(script))
        def expected1 = submitCmd(addr: "fa", taskID: 2)
        def expected2 = submitCmd(addr: "fb", taskID: 3)

        then:
        cmd1.join(' ') == expected1.join(' ')
        cmd2.join(' ') == expected2.join(' ')
    }
}
