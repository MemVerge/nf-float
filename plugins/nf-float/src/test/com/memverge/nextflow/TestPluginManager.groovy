package com.memverge.nextflow

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.plugin.DevPluginManager
import org.pf4j.CompoundPluginRepository
import org.pf4j.PluginDescriptorFinder
import org.pf4j.PluginLoader
import org.pf4j.PluginRepository

import java.nio.file.Path

@Slf4j
@CompileStatic
class TestPluginManager extends DevPluginManager{

    private Path pluginRoot

    TestPluginManager(Path root) {
        super(root)
        this.pluginRoot = root
    }

    @Override
    protected PluginDescriptorFinder createPluginDescriptorFinder() {
        return new TestPluginDescriptorFinder()
    }

    @Override
    protected PluginLoader createPluginLoader() {
        return new TestPluginLoader(this)
    }

    @Override
    protected PluginRepository createPluginRepository() {
        def repos = new CompoundPluginRepository()
        // main dev repo
        final root = getPluginsRoot()
        log.debug "Added plugin root repository: ${root}"
        repos.add( new PluginRepository() {
            @Override
            List<Path> getPluginPaths() {
                return List.of(pluginRoot)
            }

            @Override
            boolean deletePluginPath(Path pluginPath) {
                log.debug "Test mode -- Ignore deletePluginPath('$pluginPath')"
                return false
            }
        })

        return repos
    }
}
