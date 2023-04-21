package com.memverge.nextflow

import org.pf4j.ManifestPluginDescriptorFinder

import java.nio.file.Files
import java.nio.file.Path

/**
 * Plugin finder specialised for test environment. It looks for
 * the plugin 'MANIFEST.MF' file in the 'testFixtures' resources directory
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TestPluginDescriptorFinder extends ManifestPluginDescriptorFinder {

    @Override
    protected Path getManifestPath(Path pluginPath) {
        if (Files.isDirectory(pluginPath)) {
            final manifest = pluginPath.resolve('build/resources/testFixtures/META-INF/MANIFEST.MF')
            return Files.exists(manifest) ? manifest : null
        }

        return null;
    }
}