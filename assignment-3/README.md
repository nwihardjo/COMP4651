# COMP4651 Assignment-3 (6 marks)

### Deadline: Nov 2, 23:59 (Friday)
---

## MapReduce Programming

In this assignment, you will write MapReduce code that counts the bigrams appeared in a text file, using the two design patterns taught in class. Please follow these instructions carefully!

### Environment Setup
Launch the [QuickStart VM][QuickStarts] using VirtualBox and allocate it 4 GB memory. In the VM, open the Terminal and clone this repo. In particular, you may create a directory called `comp4651` in the home folder: `mkdir comp4651`. You can `cd` into that directory and `git clone` this repo, which will download the sample and skeleton code to an `assignment-3-yourGitHubHandle` folder. In case that you've received an HTTP 403 Forbidden message, just provide your GitHub username and password, i.e., `git clone https://yourGitHubHandle@github.com/hkust-comp4651-2018f/assignment-3-yourGitHubHandle`. To avoid typing in your username and password everytime you interact with GitHub, you may set up SSH connection via the private key authentication, following [these steps](http://stackoverflow.com/questions/8588768/git-push-username-password-how-to-avoid).

This assignment repo is generated using [Apache Maven][Maven]. You'll find a `pom.xml` file in the root directory (which tells [Maven][Maven] how to build the code) and a source folder `src/`. Inside `src/`, there are two sub-directories, one for the main source code (`main/`) and another for the test code (`test/`). The test code is nothing but a place holder and will not be used in this assignment. The "meat" locates in the source folder, where you'll find the following Java class files: 

* WordCount sample code: `WordCount.java`
* **Skeleton code to be completed in this assignment:** `BigramCountPairs.java`, `BigramCountStripes.java`, `BigramFrequencyPairs.java`, `BigramFrequencyStripes.java`
* Useful data structures: `PairOfStrings.java`, `HashMapStringIntWritable.java`
* Result checking programs: `AnalyzeWordCount.java`, `AnalyzeBigramCount.java`, `AnalyzeBigramFrequency.java`

You may want to use Java IDEs such as [Eclipse](Eclipse) or [IntelliJ](IntelliJ) to boost your coding/debugging efficiency. Eclipse is pre-installed in the QuickStart VM. To use it, go to the assignment-3 directory and generate an Eclipse project using Maven:
```
$ mvn eclipse:eclipse
```
Now open Eclipse. In the "File" menue, click "Import...". Expand the "General" tab, choose "Existing Projects into Workspace", and select root directory as `assignment-3-yourGitHubHandle`. This will import the Maven-generated Eclipse project into the workspace.

### Warming up with WordCount

The assignment includes a sample WordCount code for you to get started. To try it out, let's `cd` into the `assignment-3-yourGitHubHandle` folder under which `pom.xml` is located. We can build the entire project by typing
```
$ mvn clean package
```
The build should succeed without error. Before we run the WordCount example, we need to copy a text file, say, `1400-8.txt` (Project Gutenberg EBook of Great Expectations, by Charles Dickens), to HDFS:
```
$ hadoop fs -put 1400-8.txt .
```
We can now run the WordCount example to count the occurance of each word in the file we've just copied:
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.WordCount -input 1400-8.txt -output wc -numReducers 2
```
> Hadoop will print out a wealth of runtime information to track the job execution. Examining this information will give you a clear idea about what's going on there.

In the command above, we have specified the input file (`1400-8.txt`), the output directory (`wc`), and the number of reduce tasks in the Hadoop job. Since we have launched 2 reduce tasks, they respectively produced two splits in the `wc` directory, namely, `part-r-00000` and `part-r-00001`:
```
$ hadoop fs -ls wc
```
Along with the two splits, there is an empty file called `_SUCCESS`, which indicates that the Hadoop job has completed *successfully*.

We have provided a handy tool (`AnalyzeWordCount.java`) to analyze the output:
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.AnalyzeWordCount -input wc
```
The tool scans the output in `wc` and generates the following statistical summary:
```
total number of unique words: 22183
total number of words: 187462
number of words that appear only once: 13167

ten most frequent words: 
the 7885
and 6509
I	5670
to	5018
of	4446
a	3961
in	2847
was	2685
that	2648
had	2056
```

Alternatively, we can examine the details by copying the results to the local disk. For example:
```
$ hadoop fs -get wc .
$ head wc/part-r-00000
"'Eat	1
"'God	1
"'She	1
"'What	1
"(I'm	1
"--At	1
"--By	1
"--Had	1
"--Invest	1
"--That	1
```

Now it's time to examine the WordCount code in detail.

## Assignment
This assignment is a simple extension of the WordCount example: in the first part of the exercise, you'll count bigrams, and in the second part of the exercise, you'll compute bigram relative frequencies.

### Part 1 (2 marks): Count the Bigrams
[Bigrams](https://en.wikipedia.org/wiki/Bigram) are simply sequences of two consecutive words. For example, the following famous quotes from Charles Dickens' "A Tale of Two Cities"
>It was the best of times

>It was the worst of times

contain 7 bigrams: "It was", "was the", "the best", "best of", "of times", "the worst", and "worst of".

**Your job is to count the occurances of each bigram.** In particular, you will build two versions of the program:

1. (**1 mark**) *A "pairs" implementation.* The implementation must use combiners. You will build the implementation atop the skeleton code in `BigramCountPairs.java`. You will be using the data type provided in `PairOfStrings.java`.
2. (**1 mark**) *A "stripes" implementation.* The implementation must use combiners. You will build the implementation atop the skeleton code in `BigramCountStripes.java`. You will be using the data type provided in `HashMapStringIntWritable.java`.

**Output format:** Each line contains a bigram and its count, delimited by tab (`"\t"`). For example, in the previous quotes, bigram "It was" appears twice, and the corresponding output line should be:
```
It  was 2
```
where the three terms are tab-delimited (`"\t"`).

Once you are done coding, you can build your programs by `mvn clean package`, and run them:
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.BigramCountPairs -input 1400-8.txt -output bc -numReducers 2
```
To debug, you can either examine the output manually or use the analysis tool provided in `AnalyzeBigramCount.java`:
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.AnalyzeBigramCount -input bc
```
As a reference, you should see something like this:
```
total number of unique bigrams: 89925
total number of bigrams: 171207
number of bigrams that appear only once: 72438

ten most frequent bigrams: 
of	the	820
in	the	724
I	had	579
that	I	503
I	was	452
and	I	398
to	the	393
to	be	390
on	the	374
at	the	374
```
Note that the 9th and the 10th most frequent bigrams in your output might be different, but their occurrances should be 374. You may try other input text and debug yourself.

### Part 2 (4 marks): From bigram counts to relative frequencies
Extend your program to compute bigram relative frequencies, i.e., how likely you are to observe a word given the preceding word. The output of the code should be a table of values for P(W<sub>n</sub>|W<sub>n-1</sub>), where W<sub>n</sub> is the n-th word in the documents. For example, in the previous quotes from "A Tale of Two Cities", it is easy to verify that P("was"|"It") = 1 and P("best"|"the") = P ("worst"|"the") = 0.5.

Similar to Part 1, you will build two versions of the program:

1. (**2 marks**) *A "pairs" implementation.* The implementation must use combiners and partitioners. You will start with the skeleton code in `BigramFrequencyPairs.java`. You will be using the data type provided in `PairOfStrings.java`.
2. (**2 marks**) *A "stripes" implementation.* The implementation must use combiners. You will start with the skeleton code in `BigramFrequencyStripes.java`. You will be using the data type provided in `HashMapStringIntWritable.java`.

*Hint:* To compute P(B|A), count up the number of occurrences of the bigram "A B", and then divide by the number of occurrences of all the bigrams that start with "A".

**Output format:** For each word "A", the program first outputs its total count, followed by a list of relative frequencies of its succeeding words, one record per line. For example, the bigram relative frequencies of the quotes from "A Tale of Two Cities" should look something like this, where all items are separated by tab (`"\t"`):
```
It		2.0
It	was	1.0
best		1.0
best	of	1.0
of		2.0
of	times	1.0
the		2.0
the	best	0.5
the	worst	0.5
was		2.0
was	the	1.0
worst		1.0
worst	of	1.0
```
We can tell from the output above that "It" has 2 occurances, both succeeded by "was", making P("was"|"It") = 1. On the other hand, "the" has 2 occurances, one succeeded by "best" and another by "worst". As a result, we have P("best"|"the") = P("worst"|"the") = 0.5.

**It is important to follow the output format strictly as your program will be marked by an autograder!**

Once you are done coding, you can build your programs by `mvn clean package`, and run them:
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.BigramFrequencyPairs -input 1400-8.txt -output bc -numReducers 2
```
In the command above, we have specified the input file, the output path, and the number of reducers. You are free to try something different.

To debug, we have provided a handy tool (`AnalyzeBigramFrequency.java`) which you can use to produce a statistical summary of your output. For example, if you are interested in bigrams starting with "the", you can extract the ten most frequent occurrances by
```
$ hadoop jar target/assignment-3-1.0-SNAPSHOT.jar hk.ust.comp4651.AnalyzeBigramFrequency -input bc -word the
Ten most frequent bigrams starting with the:
the		7216.0
the	same	0.011086474
the	old	0.009284923
the	first	0.007899113
the	best	0.006236142
the	two	0.00595898
the	other	0.00595898
the	whole	0.0054046563
the	time	0.0052660755
the	way	0.0051274947
the	man	0.0051274947
```
You might see different output for the last two terms, but their relative frequencies should be the same as the reference output.

### Grading
We will clone your repo, go into your `assignment-3-yourGitHubHandle/` directory, and build your Maven artifact. We will run the four programs (i.e., `BigramCountPairs.java`, `BigramCountStripes.java`, `BigramFrequencyPairs.java`, and `BigramFrequencyStripes.java`) with input test cases (different from `1400-8.txt`) and compare the results with the standard output.


[QuickStarts]: https://www.cloudera.com/downloads/quickstart_vms/5-13.html
[Maven]: https://maven.apache.org
[Eclipse]: https://eclipse.org
[IntelliJ]: https://www.jetbrains.com/idea/
