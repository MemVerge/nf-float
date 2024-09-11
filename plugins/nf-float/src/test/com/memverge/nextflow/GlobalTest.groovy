package com.memverge.nextflow

import nextflow.processor.TaskConfig

import java.nio.file.Paths

class GlobalTest extends FloatBaseTest {
    def "get env var. of any names"() {
        given:
        setEnv("Bob", "A")
        setEnv("Robert", "B")
        setEnv("Peter", "C")

        when:
        def res0 = Global.getEnvFromAny(["bad", "good", "Bob"])
        def res1 = Global.getEnvFromAny(["Robert", "Peter"])

        then:
        res0 == "A"
        res1 == "B"

        cleanup:
        setEnv("Bob")
        setEnv("Robert")
        setEnv("Peter")
    }

    def "get map value from any keys"() {
        given:
        def map = [a: "1", b: "", c: "3"]

        when:
        def res0 = Global.getMapValueFromAnyKey(map, ["bad", "good", "a"])
        def res1 = Global.getMapValueFromAnyKey(map, ["b", "c"])

        then:
        res0 == "1"
        res1 == "3"
    }

    def "aws credentials not available"() {
        given:
        def config = [:]

        when:
        def res0 = Global.getAwsCredentials(config)

        then:
        res0.isValid() == false
    }

    def "retrieve aws credentials from nextflow config"() {
        given:
        def config = [aws: [
                accessKey: "A",
                secretKey: "B",
                token    : "C"]]

        when:
        def res = Global.getAwsCredentials(config)

        then:
        res.isValid() == true
        res.toString() == "accesskey=A,secret=B,token=C"
    }

    def "retrieve aws credentials from system env, no token"() {
        given:
        setEnv("AWS_ACCESS_KEY", "A")
        setEnv("AWS_SECRET_KEY", "B")
        def config = [aws:[]]

        when:
        def res = Global.getAwsCredentials(config)

        then:
        res.isValid() == true
        res.toString() == "accesskey=A,secret=B"

        cleanup:
        setEnv("AWS_ACCESS_KEY")
        setEnv("AWS_SECRET_KEY")
    }

    def setAwsEnvs() {
        setEnv("AWS_ACCESS_KEY", "A")
        setEnv("AWS_SECRET_KEY", "B")
        setEnv("AWS_SESSION_TOKEN", "C")
    }

    def clearAwsEnvs() {
        setEnv("AWS_ACCESS_KEY")
        setEnv("AWS_SECRET_KEY")
        setEnv("AWS_SESSION_TOKEN")
    }

    def "retrieve aws credentials from system env, with token"() {
        given:
        setAwsEnvs()
        def config = [aws:[]]

        when:
        def res = Global.getAwsCredentials(config)

        then:
        res.isValid() == true
        res.toString() == "accesskey=A,secret=B,token=C"
        res.opts == ["accesskey=A", "secret=B", "token=C"]

        cleanup:
        clearAwsEnvs()
    }

    def "update arg map with aws credential"() {
        given:
        setAwsEnvs()
        def config = [aws:[]]

        when:
        def res = Global.getAwsCredentials(config)
        def argMap = res.updateMap(['banana':'apple'])

        then:
        res.isValid() == true
        argMap['accesskey'] == "A"
        argMap['secret'] == "B"
        argMap['token'] == "C"
        argMap['banana'] == "apple"

        cleanup:
        clearAwsEnvs()
    }

    def "update env map with aws credential"() {
        given:
        setAwsEnvs()
        def config = [aws:[]]

        when:
        def res = Global.getAwsCredentials(config)
        def envMap = res.updateEnvMap([:], "banana")

        then:
        res.isValid() == true
        envMap['AWS_ACCESS_KEY_ID'] == "{secret:AWS_ACCESS_KEY_ID_BANANA}"
        envMap['AWS_SECRET_ACCESS_KEY'] == "{secret:AWS_SECRET_ACCESS_KEY_BANANA}"
        envMap['AWS_SESSION_TOKEN'] == "{secret:AWS_SESSION_TOKEN_BANANA}"

        cleanup:
        clearAwsEnvs()
    }

    def "add aws to path if s3 credential is available"() {
        given:
        setAwsEnvs()
        final exec = newTestExecutor()
        final task = newTask(exec, 0)

        when:
        def script = exec.getHeaderScript(task)

        then:
        script.contains("PATH:/opt/aws/dist")

        cleanup:
        clearAwsEnvs()
    }
}
