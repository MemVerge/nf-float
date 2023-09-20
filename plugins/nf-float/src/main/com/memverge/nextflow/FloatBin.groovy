package com.memverge.nextflow

import groovy.util.logging.Slf4j
import org.apache.commons.lang.SystemUtils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern

@Slf4j
class FloatBin {
    private static final binName = 'float'

    static Path get(String opCenterAddr) {
        def ret = getFloatBinPath()
        if (ret == null) {
            if (!opCenterAddr) {
                // no where to retrieve the binary
                return Paths.get(binName)
            }
            final URL src = getDownloadUrl(opCenterAddr)
            final Path pluginsDir = Global.getPluginsDir()
            ret = pluginsDir.resolve(binName)
            try {
                log.info "try downloading $src to $ret"
                Global.download(src, ret)
                ret.setExecutable(true)
            } catch (Exception ex) {
                log.warn("download ${binName} failed: ${ex.message}")
                return Paths.get(binName)
            }
        }
        return ret
    }

    private static URL getDownloadUrl(String opCenter) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new URL("https://${opCenter}/float.windows_amd64")
        } else if (SystemUtils.IS_OS_LINUX) {
            return new URL("https://${opCenter}/float")
        } else if (SystemUtils.IS_OS_MAC) {
            return new URL("https://${opCenter}/float.darwin_amd64")
        }
        throw new UnsupportedOperationException("OS not supported")
    }

    private static Path getFloatBinPath() {
        final sep = Pattern.quote(File.pathSeparator)
        def paths = [Global.getPluginsDir().toString()]
        paths.addAll(Arrays.asList(System.getenv("PATH").split(sep)))
        for (String path : paths) {
            def floatPath = Paths.get(path).resolve(binName)
            if (Files.exists(floatPath)) {
                log.info "found float binary in $path"
                return floatPath
            }
        }
        log.info "${binName} binary not found"
        return null
    }
}
