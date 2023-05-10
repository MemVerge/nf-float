package com.memverge.nextflow

import nextflow.Session
import spock.lang.Specification

class FloatFactoryTest extends Specification {
    def 'should return observer' () {
        when:
        def result = new FloatFactory().create(Mock(Session))

        then:
        result.size() == 0
    }
}
