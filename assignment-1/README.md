# COMP4651 Assignment-1 (2 questions, 6 marks)

### Deadline: Oct 5, 23:59 (Friday)
---

### Name: Nathaniel Wihardjo
### Student Id: 20315011
---

## Question 1: Measure the EC2 CPU and Memory performance

1. (1 mark) Report the name of measurement tool used in your measurements (you are free to choose any open source measurement software as long as it can measure CPU and memory performance). Please describe your configuration of the measurement tool, and explain why you set such a value for each parameter. Explain what the values obtained from measurement results represent (e.g., the value of your measurement result can be the execution time for a scientific computing task, a score given by the measurement tools or something else).

    > The used measurement tool is `sysbench`. which is an open-source modular, cross-platform and multi-threeaded benchmarking tool. The configuration for **CPU test** is to verify prime numbers by doing standard division of the number by all numbers between 2 and the square root of 20000 (which is 200) and should the remainder be 0, the next number is calculated over 2000 threads (calculate the prime numbers 2000 times concurrently using `sysbench --test=cpu --num-threads=2000 --cpu-max-prime=20000 run`. The configurations will be able to show the significant different of different size of EC2 instances computing power which put some stress on a very limited set of the CPUs features through the total time taken (from the start of the request until the end with the assumed negligible overhead shared memory access for the threads). For **memory test**, write memory operations are used with the memory total size of 150GB for which the operation is not cached in-memory with the global memory scope over 10 threads (which the memory benchmarking operations are done 10 times assumed concurrently) through `sysbench --test=memory --num-threads=10 --memory-total-size=150G --memory-scope=global run`. The difference in the memory performance can be observed through the throughput (MB/sec).   

2. (1 mark) Run your measurement tool on general purpose `t3.medium`, `m4.large`, and `m5.large` Linux instances, respectively, and find the performance differences among these instances. Launch all instances in the **US East (N. Virginia)** region. Does the performance of EC2 instances increase commensurate with the increase of the number of ECUs and memory resource?  

    In order to answer this question, you need to complete the following table by filling out blanks with the measurement results corresponding to each instance type.

    | Size      | CPU performance | Memory performance |
    |-----------|-----------------|--------------------|
    | `t3.medium` |  16.38s         |    3451.78MB/sec   |
    | `m4.large`  |  16.79s         |     974.17MB/sec   |
    | `m5.large` |   15.52s         |    3610.32MB/sec    |
 
    > Region: US East (N. Virginia)

## Question 2: Measure the EC2 Network performance

1. (1 mark) The metrics of network performance include **TCP bandwidth** and **round-trip time (RTT)**. Within the same region, what network performance is experienced between instances of the same type and different types? In order to answer this question, you need to complete the following table.  

    | Type          | TCP b/w (Mbps) | RTT (ms) |
    |---------------|----------------|----------|
    | `t3.medium`-`t3.medium` |     4770        |   0.010  |
    | `m4.large`-`m4.large`  |       566       |   0.183  |
    | `m5.large`-`m5.large` |      9600          |   0.002    |
    | `t3.medium`-`m5.large`   |   2270         |  0.011     |
    | `m4.large`-`m5.large`  |      566       |   0.212    |
    | `m4.large`-`t3.medium` |      566       |   0.191    |

    > Region: US East (N. Virginia)

2. (1 mark) What about the network performance for instances deployed in different regions? In order to answer this question, you need to complete the following table.

    | Connection | TCP b/w (Mbps)  | RTT (ms) |
    |------------|-----------------|--------------------|
    | N. Virginia-Tokyo |      10.4       |     0.004               |
    | N. Virginia-N. Virginia  |     9600        |      0.002         |
    | Tokyo-Tokyo |      9600       |      0.001         |
 
    > All instances are `m5.large`.
 
3. (1 mark) Is the network performance consistent over time? In order to answer this question, you need to complete the following table.

    | Time | TCP b/w (Mbps)  | RTT (ms) |
    |------------|-----------------|--------------------|
    | Morning |                 |                    |
    | Afternoon  |     9600        |      0.002         |
    | Evening |                 |                    |
 
    > Region: US East (N. Virginia); Connection: `m5.large`-`m5.large`
 
4. (1 mark) *Open-end question:* In the above three sub-questions, you have measured network performance in different scenarios. Observe the values in each table, and try to explain why the network performance varies in different scenarios?

    > The TCP measured using `iPerf` is the throughput bandwidth which measures the average rate of the successful data transfer. Based on table 1, it can be observed that the network performances between the same type of instances perform the best, while different types of the AWS EC2 instances result in worse performance than the same type. However it can be noticed that the `m4.large` type has the worst networking performance which has lowest TCP b/w and highest RTT over all EC2 instance types. The bandwidth is determined by the **client** of the connection. Instances deployed in different regions also contribute to the network performance as the data being sent travels more distance, and thus, result in lower TCP b/w as well as higher RTT. *RTT* represents the travel time between a package which is sent from the client to the server, and back to the client. The lower the RTT represents faster network. 
