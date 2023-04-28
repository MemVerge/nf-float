package com.memverge.nextflow

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
}
