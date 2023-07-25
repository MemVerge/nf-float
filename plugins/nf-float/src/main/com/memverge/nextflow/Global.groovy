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
import nextflow.util.IniFile

import java.nio.file.Path
import java.nio.file.Paths

/**
 * The Global class is copied from the same class in NextFlow.
 * We copy some implementation because they are updated in different releases.
 */
@Slf4j
class Global {

    @PackageScope
    static List<String> getAwsCredentials(Map env, Map config) {

        def home = Paths.get(System.properties.get('user.home') as String)
        def files = [ home.resolve('.aws/credentials'), home.resolve('.aws/config') ]
        getAwsCredentials0(env, config, files)

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
     *      {@code null} if the credentials are missing
     */
    @PackageScope
    static List<String> getAwsCredentials0( Map env, Map config, List<Path> files = []) {

        String a
        String b

        if( config && config.aws instanceof Map ) {
            a = ((Map)config.aws).accessKey
            b = ((Map)config.aws).secretKey

            if( a && b ) {
                log.debug "Using AWS credentials defined in nextflow config file"
                return [a, b]
            }

        }

        // as define by amazon doc
        // http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
        if( env && (a=env.AWS_ACCESS_KEY_ID) && (b=env.AWS_SECRET_ACCESS_KEY) )  {
            log.debug "Using AWS credentials defined by environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            return [a, b]
        }

        if( env && (a=env.AWS_ACCESS_KEY) && (b=env.AWS_SECRET_KEY) ) {
            log.debug "Using AWS credentials defined by environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY"
            return [a, b]
        }

        for( Path it : files ) {
            final conf = new IniFile(it)
            final profile = getAwsProfile0(env, config)
            final section = conf.section(profile)
            if( (a=section.aws_access_key_id) && (b=section.aws_secret_access_key) ) {
                final token = section.aws_session_token
                if( token ) {
                    log.debug "Using AWS temporary session credentials defined in `$profile` section in file: ${conf.file}"
                    return [a,b,token]
                }
                else {
                    log.debug "Using AWS credential defined in `$profile` section in file: ${conf.file}"
                    return [a,b]
                }
            }
        }

        return null
    }

    static protected String getAwsProfile0(Map env, Map<String,Object> config) {

        final profile = config?.navigate('aws.profile')
        if( profile )
            return profile

        if( env?.containsKey('AWS_PROFILE'))
            return env.get('AWS_PROFILE')

        if( env?.containsKey('AWS_DEFAULT_PROFILE'))
            return env.get('AWS_DEFAULT_PROFILE')

        return 'default'
    }
}
