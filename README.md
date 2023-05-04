# nf-float plugin

This project contains the Nextflow plugin for MemVerge MMCE(float).

This plugin implements the `float` executor and allows Nextflow to execute the
workload with `float`.

`FloatGridExecutor` extends the `AbstractGridExecutor` and tells Nextflow how
to run the workload with `float` command line.  It requires the user to set up
proper NFS for all workers.

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


### Install nf-float plugin

Go to the folder where you just install the `nextflow` command line.
Let's call this folder the Nextflow home directory.
Create the float plugin folder with:
```
mkdir -p .nextflow/plugins/nf-float-0.1.3
```
where `0.1.3` is the version of the float plugin.  This version number should 
align with the version in of your plugin and the property in your configuration
file. (check the configuration section)

Retrieve your plugin zip file and unzip it in this folder.
If everything goes right, you should be able to see two sub-folders:
```
$ ll .nextflow/plugins/nf-float-0.1.3/
total 48
drwxr-xr-x 4 ec2-user ec2-user    51 Jan  5 07:17 classes
drwxr-xr-x 2 ec2-user ec2-user    25 Jan  5 07:17 META-INF
```

## Configuration

Users need to update the default configuration file or supply a configuration
file with the command line option `-c`.  Here is a sample of the configuration.

```
plugins {
    id 'nf-float@0.1.3'
}

workDir = '/mnt/memverge/shared'

float {
    address = 'opcenter.compute.amazonaws.com'
    username = 'admin'
    password = 'memverge'
    nfs = 'nfs://1.2.3.4/mnt/memverge/shared'
}
```

* In the `plugins` section, users must specify the plugin name and version.
* `workDir` is where we mount the NFS and where Nextflow put the process files.
* In the `float` section, users must supply the address of the MMCE operation
  center and the proper credentials.
  * `address` address of your operation center(s).  Separate multiple addresses with `,`.
  * `username` and `password` are the credentials for your operation center
  * `nfs` points to the location of the NFS.
  * `image` (deprecated) is an optional property that specifies the default image for a float process.
  * `container` is an optional property that specifies the default image for a float process.
  * `cpu` (deprecated) is an optional property that specifies the default number of 
    CPU cores for a float process, the default value is `2`.
  * `cpus` is an optional property that specifies the default number of
    CPU cores for a float process, the default value is `2`.
  * `mem` (deprecated) is an optional property that specifies the default size of memory for a
    float process in GB.  The default value is `4`.
  * `memory` is an optional property that specifies the default size of memory for a
    float process in GB.  The default value is `'4 GB'`.
  * `commonExtra` allows the user to specify other submit parameters.  This parameter
    will be appended to every float submit command.

## Task Sample
[FloatGridExecutor.groovy](plugins%2Fnf-float%2Fsrc%2Fmain%2Fcom%2Fmemverge%2Fnextflow%2FFloatGridExecutor.groovy)
For each process, users could supply their requirements for the CPU, memory and image.
Here is an example of a hello world workflow.

```
process sayHello {
  executor = 'float'
  image = 'cactus'
  cpu = 2
  mem = 4

  output:
    stdout

  """
  echo "Hello from NextFlow!"
  """
}

workflow {
  sayHello | view { it.trim() }
}
```

* `executor = 'float'` - tells Nextflow to run the workflow with `float`.
* `cpu` - (deprecated) specifies the number of cores required by this process.
* `cpus` - specifies the number of cores required by this process.
* `mem` - (deprecated) specifies the number of memory required by this process in GB.
* `memory` specify the memories.  Note that the value is a string, such as `'5 GB'`
* `image` - (deprecated) is the name of the container image.
* `contaner` - the same as `image`
* `extra` - specifies extra parameters for the job.  It will be merged with
            the `commonExtra` parameter. 

## Run the Workflow

Use the `nextflow` command to run the workflow.  We need to include our configuration
file and task file as arguments.  Here is an example.

```
./nextflow run samples/tutorial.nf -c conf/float-rt.conf
```

## Plugin Assets
                    
- `settings.gradle`
 
    Gradle project settings. 

- `plugins/nf-float`
    
    The plugin implementation base directory.

- `plugins/nf-float/build.gradle` 
    
    Plugin Gradle build file

- `plugins/nf-float/src/resources/META-INF/MANIFEST.MF` 
    
    Manifest file defining the plugin attributes.

- `plugins/nf-float/src/resources/META-INF/extensions.idx`
    
    This file declares one or more extension classes provided by the plugin.

- `plugins/nf-float/src/main` 

    The plugin implementation sources.

- `plugins/nf-float/src/test` 
                             
    The plugin unit tests. 

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

```
./gradlew makeZip
```

The output is available at `./plugins/nf-float/build/libs/`
