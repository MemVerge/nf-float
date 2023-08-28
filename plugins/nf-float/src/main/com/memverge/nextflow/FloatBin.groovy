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
        if (!opCenterAddr) {
            return Paths.get(binName)
        }
        def ret = getFloatBinPath()
        if (ret == null) {
            final URL src = getDownloadUrl(opCenterAddr)
            final Path pluginsDir = Global.getPluginsDir()
            ret = pluginsDir.resolve(binName)
            try {
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
        def paths = Arrays.asList(System.getenv("PATH").split(sep))
        paths = new ArrayList<String>(paths)
        paths.add(Global.getPluginsDir().toString())
        for (String path : paths) {
            def floatPath = Paths.get(path).resolve(binName)
            if (Files.exists(floatPath)) {
                return floatPath
            }
        }
        log.info "${binName} binary not found"
        return null
    }
}
