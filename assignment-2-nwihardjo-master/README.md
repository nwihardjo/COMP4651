# COMP4651 Assignment-2 (2 marks)

### Deadline: Oct 14, 23:59 (Sunday)
---

## HDFS APIs

In this assignment, you will write a simple code that copies a file from HDFS to the local disk, using HDFS APIs. Please follow these instructions carefully!

First, download the [Cloudera QuickStarts VM (CDH 5.13)][QuickStarts]. The VM includes a complete set of Hadoop software distributions which provides a ready-to-use single-node cluster for the hands-on labs. Please be noted that the downloading may take a few hours, depending on your network conditions.

Launch the [QuickStarts VM][QuickStarts] using VirtualBox and allocate it 4 GB memory. To do so, right-click the VM and choose "Settings…" from the pop-up window. Navigate to the "System" tab and set the base memory to 4 GB. We also recommend using 32 MB video memory. You can do the setting under the "Display" tab. The VM runs CentOS. Once it starts, you will be navigated to a web page that shows you a beginner tutorial of CDH softwares. Learning this tutorial is out of the scope of this course, but you are welcome to walk through it if you are interested.

In the VM, open the Terminal and clone this repo. In particular, you may create a directory called `comp4651` in the home folder: `mkdir comp4651`. You can `cd` into that directory and run `sudo yum update -y nss curl libcurl` to install needed libraries. You can now `git clone` the assignment repo, which will download the sample and skeleton code to an `assignment-2-yourGitHubHandle` folder. In case that you've received an HTTP 403 Forbidden message, just provide your GitHub username and password, i.e., `git clone https://yourGitHubHandle@github.com/hkust-comp4651-2018f/assignment-2-yourGitHubHandle`. To avoid typing in your username and password everytime you interact with GitHub, you may set up SSH connection via the private key authentication, following [these steps](http://stackoverflow.com/questions/8588768/git-push-username-password-how-to-avoid).

This assignment repo is generated using [Apache Maven][Maven], a software project management and comprehension tool. If you examine the structure of the repo using `tree assignment-2-yourGitHubHandle`, you'll find a `pom.xml` file in the root directory (which tells [Maven][Maven] how to build the code) and a source folder `src/`. Inside `src/`, there are two sub-directories, one for the main source code (`main/`) and another for the test code (`test/`). The test code is nothing but a place holder and will not be used in this assignment. The "meat" locates in the source folder, where you'll find three Java class files: `CopyFile.java` (which copies a file in a specified filesystem to another), `CopyLocalFile.java` (which copies a local file to HDFS), and `FileSystemCat.java` (which prints out the file contents). The latter two are complete example code that are well documented, while `CopyFile.java` is just a *skeleton* that has not been implemented.

**Your job is to complete the skeleton** `CopyFile.java` **with which your can copy a file from HDFS to a local disk.**

To try out the sample code, let's `cd` into the `assignment-2-yourGitHubHandle` folder under which `pom.xml` is located. You can now build the Maven project and package it to a jar file with the following command:
```
$ mvn clean package
```
Once the build succeeds, you should be able to run the two samples. Let's first copy a local file to HDFS:
```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.comp4651.CopyLocalFile hkust.txt hkust.txt
```
You will find a copy of `hkust.txt` file in HDFS:
```
$ hadoop fs -ls
```
You can now print out the contents of this file in HDFS:
```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.comp4651.FileSystemCat hkust.txt
```
The output should match the local copy: `cat hkust.txt`.

Now it's time to examine the two example code in details and complete your `CopyFile.java`. After you are done, you can build the Maven project with `mvn clean package` and do some tests. For example, you can copy back `hkust.txt` from HDFS to the local disk:
```
$ export LOCAL_DIR=file:///`pwd`
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.comp4651.CopyFile hkust.txt $LOCAL_DIR/cp-hkust.txt
```
> In [QuickStarts VM][QuickStarts], the default filesystem is HDFS. If your want to refer to a local filesystem, you need to specify a prefix `file:///` before a file path.

If your implementation is correct, you will find a `cp-hkust.txt` file in your current directory, with the same contents as `hkust.txt`. You can try to copy other files for further tests.

When you are done, make sure you have committed your code and pushed the repo back to origin.

> You may want to use Java IDEs such as [Eclipse][Eclipse] or [IntelliJ][IntelliJ] to boost your coding/debugging efficiency. For example, [Eclipse][Eclipse] is pre-installed in the [QuickStarts VM][QuickStarts]. To use it, go to the `assignment-2` directory and generate an Eclipse project using Maven:
```
$ mvn eclipse:eclipse
```
Now open Eclipse. In the "File" menue, click "Import...". Expand the "General" tab, choose "Existing Projects into Workspace", and select root directory as `assignment-2`. This will import the Maven-generated Eclipse project into the workspace.

### Grading

Here's how we are going to grade your assignment. we will clone your repo, go into your `assignment-2-yourGitHubHandle/` directory, and build your Maven artifact:
```
$ mvn clean package
```
We will then generate a random dummy file, say 100 MB, and calculate its md5sum:
```
$ head -c 100M < /dev/urandom > dummy
$ md5sum dummy > md5.txt
```
We shall then move the dummy file to HDFS and copy it back using your code:
```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.comp4651.CopyFile dummy $LOCAL_DIR/dummy
```
Finally, we calculate the md5sum of the file and check if it matches that of the original copy:
```
$ md5sum -c md5.txt
```

[QuickStarts]: https://www.cloudera.com/downloads/quickstart_vms/5-13.html
[Maven]: https://maven.apache.org
[Eclipse]: https://eclipse.org
[IntelliJ]: https://www.jetbrains.com/idea/
