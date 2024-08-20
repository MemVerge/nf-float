package com.memverge.nextflow

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

    def "retrieve aws credentials from system env, with token"() {
        given:
        setEnv("AWS_ACCESS_KEY", "A")
        setEnv("AWS_SECRET_KEY", "B")
        setEnv("AWS_SESSION_TOKEN", "C")
        def config = [aws:[]]

        when:
        def res = Global.getAwsCredentials(config)

        then:
        res.isValid() == true
        res.toString() == "accesskey=A,secret=B,token=C"
        res.opts == ["accesskey=A", "secret=B", "token=C"]

        cleanup:
        setEnv("AWS_ACCESS_KEY")
        setEnv("AWS_SECRET_KEY")
        setEnv("AWS_SESSION_TOKEN")
    }
}
