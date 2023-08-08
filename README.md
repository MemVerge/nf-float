# nf-float plugin

This project contains the Nextflow plugin for MemVerge Memory Machine Cloud 
(aka. float).

`FloatGridExecutor` extends the `AbstractGridExecutor` and tells Nextflow how
to run the workload with `float` command line.  

Please make sure your nextflow node shares the same work directory with the
worker nodes/containers.  It should be a shared file system such as NFS or S3FS.

Otherwise, the worker nodes won't be able to see the task files.

## License

[Apache License Version 2.0](./LICENSE)

## Installation

To run `float` with Nextflow, you must install the Nextflow and the float plugin.

__Note__

Nextflow and the plugin should be installed on a node that have 
access to the NFS which is available to all worker nodes.

### Install Nextflow

Enter this command in your terminal:
```
curl -s https://get.nextflow.io | bash
```
It creates a file `nextflow` in the current dir.

__Note__:

Nextflow requires java 11 or higher.  You may need to install openjdk 11 
for your environment.

You could always find the latest installation
guide at https://www.nextflow.io/docs/latest/getstarted.html.


### Install nf-float plugin

#### Auto Install

The `nf-float` plugin is available on the Nextflow community plugins site.
When Nextflow sees following configuration, it will automatically download
the plugin.  

Just make sure you have proper internet access.

```groovy
plugins {
    id 'nf-float@0.3.0'
}
```

#### Manual Install

Sometimes you want to deploy a customized plugin.  In this case, you can
install it manually.

Go to the folder where you just install the `nextflow` command line.
Let's call this folder the Nextflow home directory.
Create the float plugin folder with:
```bash
mkdir -p .nextflow/plugins/nf-float-0.3.0
```
where `0.3.0` is the version of the float plugin.  This version number should 
align with the version in of your plugin and the property in your configuration
file. (check the configuration section)

Retrieve your plugin zip file and unzip it in this folder.
If everything goes right, you should be able to see two sub-folders:

```bash
$ ll .nextflow/plugins/nf-float-0.3.0/
total 48
drwxr-xr-x 4 ec2-user ec2-user    51 Jan  5 07:17 classes
drwxr-xr-x 2 ec2-user ec2-user    25 Jan  5 07:17 META-INF
```

## Configuration

Users need to update the default configuration file or supply a configuration
file with the command line option `-c`.  Here is a sample of the configuration.

```groovy
plugins {
    id 'nf-float@0.3.0'
}

workDir = '/mnt/memverge/shared'

float {
    address = 'opcenter.compute.amazonaws.com'
    username = 'admin'
    password = 'memverge'
    nfs = 'nfs://1.2.3.4/mnt/memverge/shared'
}

process {
    executor = 'float'
}
```

* In the `plugins` section, users must specify the plugin name and version.
* `workDir` is where we mount the NFS and where Nextflow put the process files.
* In the `float` section, users must supply the address of the MMCE operation
  center and the proper credentials.
* In the `process` scope, we specify `executor = 'float'` to tell Nextflow to execute
  tasks with the Float executor.

Available `float` config options: 

* `address`: address of your operation center(s).  Separate multiple addresses with `,`.
* `username`, `password`: the credentials for your operation center
* `nfs`: the location of the NFS (if using NFS for the work directory)
* `migratePolicy`: the migration policy used by WaveRider, specified as a map.  Refer to
              the CLI usage for the list of available options.
* `vmPolicy`: the VM creation policy, specified as a map.  Refer to the CLI usage
              for the list of available options.
* `commonExtra`: allows the user to specify other submit CLI options.  This parameter
                 will be appended to every float submit command.

### Configure with environment variables

The plugin allows the user to set credentials with environment variables.
If the credentials are not available in the configuration file, it will try
reading these environment variables.

* `MMC_ADDRESS` for operation center address.  Separate multiple addresses with `,`.
* `MMC_USERNAME` for login username
* `MMC_PASSWORD` for login password


### Configure with Nextflow secrets

User can use Nextflow secrets to input the credentials.  Here is an example:

```bash
nextflow secrets set MMC_USERNAME "..."
nextflow secrets set MMC_PASSWORD "..."
```

In the configuration file, you can reference the secrets like this:

```groovy
float {
    username = secrets.MMC_USERNAME
    password = secrets.MMC_PASSWORD
}
```

If the secret is not available, Nextflow reports error like this:

```
Unknown config secret 'MMC_USERNAME'
```

### Configure s3 work directory

To enable s3 as work directory, user need to set work directory to a s3 bucket.
```groovy
plugins {
    id 'nf-float@0.3.0'
}

workDir = 's3://bucket/path'

process {
    executor = 'float'
    container = 'fedora/fedora-minimal'
}

podman.registry = 'quay.io'

float {
    address = 'op.center.address'
    username = secrets.MMC_USERNAME
    password = secrets.MMC_PASSWORD
}

aws {
  accessKey = '***'
  secretKey = '***'
  region = 'us-east-2'
}
```

You don't need to specify `nfs` in `float` scope.  The plugin will assemble
the `nfs` option automatically.

The plugin retrieves the s3 credentials from Nextflow.
Nextflow looks for AWS credentials in the following order:
* the `nextflow.config` file in the pipeline execution directory
* the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
* the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
* the default profile in the AWS credentials file located at `~/.aws/credentials`
* the default profile in the AWS client configuration file located at `~/.aws/config`
* the temporary AWS credentials provided by an IAM instance role. See IAM Roles documentation for details.

For detail, check NextFlow's document.
https://www.nextflow.io/docs/latest/amazons3.html#security-credentials

Tests done for s3 work directory support:
* trivial sequence and scatter workflow.
* the test profile of nf-core/rnaseq
* the test profile of nf-core/sarek


### Configure fusion FS over s3

Since release 0.3.0, we support fusion FS over s3.  To enable fusion, you need to add following configurations
```groovy
wave.enabled = true // 1

fusion {
  enabled = true // 2
  exportStorageCredentials = true // 3
  exportAwsAccessKeys = true // 4
}
```
1. fusion needs wave support.
2. enable fusion explicitly
3. export the aws credentials as environment variable.
4. same as 3.  Different nextflow versions may require different option.  Supply both 3 & 4 if you are not sure.

In additional, you may want to:
* point your work directory to a location in s3.
* specify your s3 credentials in the `aws` scope.

When fusion is enabled, you can find similar submit command line in your `.nextflow.log`
```bash
float -a <op-center-address> -u admin -p *** submit
    --image 'wave.seqera.io/wt/dfd4c4e2d48d/biocontainers/mulled-v2-***:***-0'
    --cpu 12
    --mem 72
    --job /tmp/nextflow5377817282489183149.command.run
    --env FUSION_WORK=/fusion/s3/cedric-memverge-test/nf-work/work/31/a4b682beb93c944fbd3a342ffc41c5
    --env AWS_ACCESS_KEY_ID=***
    --env AWS_SECRET_ACCESS_KEY=***
    --env 'FUSION_TAGS=[.command.*|.exitcode|.fusion.*](nextflow.io/metadata=true),[*](nextflow.io/temporary=true)'
    --extraContainerOpts --privileged
    --customTag nf-job-id:znzjht-4
```
* the task image is wrapped by a layer provided by wave.
  __note__: releases prior to MMC 2.3.1 has bug that fails the image pull requests to the wave registry.  
  please upgrade to the latest MMC master.
* `FUSION_WORK` and `FUSION_TAGS` is added as environment variables.
* aws credentials is added as environment variables.
* use `extraContainerOpts` to make sure we run the container in privileged mode.
  __note__: this option requires MMC 2.3 or later.

Tests for the fusion support.
* trivial sequence and scatter workflow.
* the test profile of nf-core/rnaseq
* the test profile of nf-core/sarek

### Configure VM creation and migration policies

While the VM and migration policies can be specified like any CLI option via `float.commonExtra`,
they can also be specified using the config options `float.vmPolicy` and `float.migratePolicy` as maps:

```groovy
float {
    vmPolicy = [
        spotFirst: true,
        retryLimit: 3,
        retryInterval: '10m'
    ]

    migratePolicy = [
        cpu: [upperBoundRatio: 90, upperBoundDuration: '10s'],
        mem: [lowerBoundRatio: 20, upperBoundRatio: 90]
    ]
}
```

## Process definition

For each process, users could supply their requirements for the CPU, memory and container image using the standard Nextflow process directives.
Here is an example of a hello world workflow.

```groovy
process sayHello {
  executor 'float'
  container 'cactus'
  cpus 2
  memory 4.GB
  disk 50.GB
  time '1h'

  output:
    stdout

  """
  echo "Hello from Nextflow!"
  """
}

workflow {
  sayHello | view { it.trim() }
}
```

The following process directives are supported for specifying task resources:

* `conda` (only when using [Wave](https://seqera.io/wave/))
* `container`
* `cpus`
* `disk` (controls the size of the volume of the workload, minimal size is 40 GB)
* `machineType`
* `memory`
* `resourceLabels`
* `time`

## Run the Workflow

Use the `nextflow` command to run the workflow.  We need to include our configuration
file and task file as arguments.  Here is an example.

```bash
./nextflow run samples/tutorial.nf -c conf/float-rt.conf
```

## Unit testing 

Run the following command in the project root directory (ie. where the file `settings.gradle` is located):

```bash
./gradlew check
```

## Testing and debugging

To run and test the plugin in a development environment, 
configure a local Nextflow build with the following steps:

1. Clone the Nextflow repository in your computer into a sibling directory:
    ```bash
    git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow
    ```
  
2. Configure the plugin build to use the local Nextflow code:
    ```bash
    echo "includeBuild('../nextflow')" >> settings.gradle
    ```
  
   (Make sure to not add it more than once!)

3. Compile the plugin alongside the Nextflow code:
    ```bash
    ./gradlew compileGroovy
    ```

4. Run Nextflow with the plugin, using `./launch.sh` as a drop-in replacement for the `nextflow` command, and adding the option `-plugins nf-hello` to load the plugin:
    ```bash
    ./launch.sh run samples/hello.nf -c conf/float-rt.conf -plugins nf-float
    ```

## Package

Run following command to create the plugin zip.

```bash
./gradlew makeZip
```

The output is available at `./plugins/nf-float/build/libs/`
