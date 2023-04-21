package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.pf4j.BasePluginLoader
import org.pf4j.PluginManager

/**
 * Plugin loader specialised for unit testing. The plugin must be defined
 * in the sub-project 'testFixtures' source tree
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class TestPluginLoader extends BasePluginLoader {

    TestPluginLoader(PluginManager pluginManager) {
        super(pluginManager, new TestPluginClasspath())
    }

}