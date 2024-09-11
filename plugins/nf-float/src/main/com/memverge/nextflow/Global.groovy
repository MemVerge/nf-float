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

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.SysEnv
import nextflow.util.IniFile

import javax.net.ssl.*
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate

class CmdRes {
    CmdRes(int exit, String out) {
        this.exit = exit
        this.out = out
    }

    boolean getSucceeded() {
        return exit == 0
    }

    String toString() {
        return "exit code: ${exit}, out: ${out}"
    }

    int exit
    String out
}

class AWSCred {
    String accessKey
    String secretKey
    String token

    AWSCred(String accessKey, String secretKey, String token = null) {
        this.accessKey = accessKey
        this.secretKey = secretKey
        this.token = token
    }

    Boolean isValid() {
        return accessKey && secretKey
    }

    String toString() {
        def opts = getOpts()
        return opts.join(",")
    }

    private static String getS3UserSecretKey(String runName) {
        return "AWS_ACCESS_KEY_ID_${runName.toUpperCase()}"
    }

    private static String getS3PassSecretKey(String runName) {
        return "AWS_SECRET_ACCESS_KEY_${runName.toUpperCase()}"
    }

    private static String getS3tokenSecretKey(String runName) {
        return "AWS_SESSION_TOKEN_${runName.toUpperCase()}"
    }

    Map<String, String> getRunSecretsMap(String runName) {
        def ret = new HashMap<String, String>()
        if (isValid()) {
            ret.put(getS3UserSecretKey(runName), accessKey)
            ret.put(getS3PassSecretKey(runName), secretKey)
            if (token) {
                ret.put(getS3tokenSecretKey(runName), token)
            }
        }
        return ret
    }


    def updateMap(Map map) {
        if (!isValid()) {
            return map
        }
        if (hasAllKeyCaseInsensitive(map, ["accesskey", "secret"])) {
            return map
        }
        map.put("accesskey", accessKey)
        map.put("secret", secretKey)
        if (token) {
            map.put("token", token)
        }
        return map
    }

    Map<String, String> updateEnvMap(Map map, String runName) {
        if (!isValid()) {
            return map
        }
        if (hasAllKeyCaseInsensitive(map, ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])) {
            return map
        }
        map.put("AWS_ACCESS_KEY_ID", "{secret:${getS3UserSecretKey(runName)}}")
        map.put("AWS_SECRET_ACCESS_KEY", "{secret:${getS3PassSecretKey(runName)}}")
        if (token) {
            map.put("AWS_SESSION_TOKEN", "{secret:${getS3tokenSecretKey(runName)}}")
        }
        return map
    }

    private static String getExportCmd(String secretName) {
        return "\$(/opt/memverge/bin/float secret get $secretName -a \$FLOAT_ADDR)"
    }

    String getExportS3CredScript(String runName) {
        def ret = ""
        if (isValid()) {
            ret = "export AWS_ACCESS_KEY_ID=${getExportCmd(getS3UserSecretKey(runName))}\n"
            ret += "export AWS_SECRET_ACCESS_KEY=${getExportCmd(getS3PassSecretKey(runName))}\n"
            if (token) {
                ret += "export AWS_SESSION_TOKEN=${getExportCmd(getS3tokenSecretKey(runName))}\n"
            }
        }
        return ret
    }

    List<String> getOpts() {
        def ret = ["accesskey=${accessKey}", "secret=${secretKey}"]
        if (token) {
            ret.add("token=${token}")
        }
        return ret
    }

    private static boolean hasAllKeyCaseInsensitive(
            Map map, Collection<String> keys) {
        final expectedSize = keys.size()
        def matched = 0
        // compare the keys in a case-insensitive way
        for (String key : keys) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (key.equalsIgnoreCase(entry.key)) {
                    matched++
                    break
                }
            }
        }
        return matched == expectedSize
    }
}


/**
 * The Global class is copied from the same class in NextFlow.
 * We copy some implementation because they are updated in different releases.
 */
@Slf4j
class Global {

    @PackageScope
    static CmdRes execute(List<String> cmd) {
        final buf = new StringBuilder()
        final process = new ProcessBuilder(cmd).redirectErrorStream(true).start()
        final consumer = process.consumeProcessOutputStream(buf)
        process.waitForOrKill(60_000)
        final exit = process.exitValue(); consumer.join() // <-- make sure sync with the output consume #1045
        final out = buf.toString()
        return new CmdRes(exit, out)
    }

    @PackageScope
    static def download(URL url, Path filename) {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    X509Certificate[] getAcceptedIssuers() { return null }

                    void checkClientTrusted(X509Certificate[] certs, String authType) {}

                    void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
        }

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL")
        sc.init(null, trustAllCerts, new SecureRandom())
        def dftFactory = HttpsURLConnection.getDefaultSSLSocketFactory()
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory())

        // Create all-trusting host name verifier
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            boolean verify(String hostname, SSLSession session) { return true }
        }

        // Install the all-trusting host verifier
        def dftVerifier = HttpsURLConnection.getDefaultHostnameVerifier()
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid)

        url.openConnection().with { conn ->
            filename.toFile().withOutputStream { out ->
                conn.inputStream.with { inp ->
                    out << inp
                    inp.close()
                }
            }
        }
        log.info "downloaded ${url} to ${filename}"
        // restore the factory and verifier
        HttpsURLConnection.setDefaultSSLSocketFactory(dftFactory)
        HttpsURLConnection.setDefaultHostnameVerifier(dftVerifier)
    }

    @PackageScope
    static Path getPluginsDir() {
        Map<String, String> env = SysEnv.get()
        final dir = env.get('NXF_PLUGINS_DIR')
        if (dir) {
            log.trace "[FLOAT] Detected NXF_PLUGINS_DIR=$dir"
            return Paths.get(dir)
        } else if (env.containsKey('NXF_HOME')) {
            log.trace "[FLOAT] Detected NXF_HOME - Using ${env.NXF_HOME}/plugins"
            return Paths.get(env.NXF_HOME, 'plugins')
        } else {
            log.trace "[FLOAT] Using local plugins directory"
            return Paths.get('plugins')
        }
    }

    @PackageScope
    static AWSCred getAwsCredentials(Map config) {

        def home = Paths.get(System.properties.get('user.home') as String)
        def files = [home.resolve('.aws/credentials'), home.resolve('.aws/config')]
        getAwsCredentials0(config, files)
    }

    static def setEnv(String key, String value) {
        try {
            def env = System.getenv()
            def cl = env.getClass()
            def field = cl.getDeclaredField("m")
            field.setAccessible(true)
            def writableEnv = (Map<String, String>) field.get(env)
            writableEnv.put(key, value)
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e)
        }
    }

    static def getEnvFromAny(Collection<String> keys) {
        def env = System.getenv()
        return getMapValueFromAnyKey(env, keys)
    }

    static def getMapValueFromAnyKey(Map map, Collection<String> keys) {
        String ret = ""
        for (String key : keys) {
            if (map.containsKey(key)) {
                ret = map.get(key)
                if (ret) {
                    break
                }
            }
        }
        return ret
    }

    /**
     * Retrieve the AWS credentials from the given context. It look for AWS credential in the following order
     * 1) Nextflow config {@code aws.accessKey} and {@code aws.secretKey} pair
     * 2) System env {@code AWS_ACCESS_KEY} and {@code AWS_SECRET_KEY} pair
     * 3) System env {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY} pair
     *
     *
     * @param env The system environment map
     * @param config The nextflow config object map
     * @return A pair where the first element is the access key and the second the secret key or
     * {@code null} if the credentials are missing
     */
    @PackageScope
    static AWSCred getAwsCredentials0(Map config, List<Path> files = []) {
        String key
        String secret
        String token
        AWSCred ret

        if (config && config.aws instanceof Map) {
            key = ((Map) config.aws).accessKey
            secret = ((Map) config.aws).secretKey
            token = ((Map) config.aws).token

            ret = new AWSCred(key, secret, token)
            if (ret.isValid()) {
                log.debug "[FLOAT] Using AWS credentials defined in nextflow config file"
                return ret
            }

        }

        // as define by amazon doc
        // http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
        key = getEnvFromAny(['AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY'])
        secret = getEnvFromAny(['AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_KEY'])
        token = getEnvFromAny(['AWS_SESSION_TOKEN', 'AWS_TOKEN'])
        ret = new AWSCred(key, secret, token)
        if (ret.isValid()) {
            log.debug "[FLOAT] Using AWS credentials defined by environment variables"
            return ret
        }

        for (Path it : files) {
            final conf = new IniFile(it)
            def env = System.getenv()
            final profile = getAwsProfile0(env, config)
            final section = conf.section(profile)
            if ((key = section.aws_access_key_id) && (secret = section.aws_secret_access_key)) {
                token = section.aws_session_token
                ret = new AWSCred(key, secret, token)
                if (ret.isValid()) {
                    log.debug "[FLOAT] Using AWS credentials defined in `$profile` section in file: ${conf.file}"
                    return ret
                }
            }
        }
        ret = new AWSCred("", "", "")
        return ret
    }

    static protected String getAwsProfile0(Map env, Map<String, Object> config) {

        final profile = config?.navigate('aws.profile')
        if (profile)
            return profile

        if (env?.containsKey('AWS_PROFILE'))
            return env.get('AWS_PROFILE')

        if (env?.containsKey('AWS_DEFAULT_PROFILE'))
            return env.get('AWS_DEFAULT_PROFILE')

        return 'default'
    }
}
