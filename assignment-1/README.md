# COMP4651 Assignment-1 (2 questions, 6 marks)

### Deadline: Oct 5, 23:59 (Friday)
---

### Name:
### Student Id:
---

## Question 1: Measure the EC2 CPU and Memory performance

1. (1 mark) Report the name of measurement tool used in your measurements (you are free to choose any open source measurement software as long as it can measure CPU and memory performance). Please describe your configuration of the measurement tool, and explain why you set such a value for each parameter. Explain what the values obtained from measurement results represent (e.g., the value of your measurement result can be the execution time for a scientific computing task, a score given by the measurement tools or something else).

    > Your answer goes here.

2. (1 mark) Run your measurement tool on general purpose `t3.medium`, `m4.large`, and `m5.large` Linux instances, respectively, and find the performance differences among these instances. Launch all instances in the **US East (N. Virginia)** region. Does the performance of EC2 instances increase commensurate with the increase of the number of ECUs and memory resource?  

    In order to answer this question, you need to complete the following table by filling out blanks with the measurement results corresponding to each instance type.

    | Size      | CPU performance | Memory performance |
    |-----------|-----------------|--------------------|
    | `t3.medium` |                 |                    |
    | `m4.large`  |                 |                    |
    | `m5.large` |                 |                    |
 
    > Region: US East (N. Virginia)

## Question 2: Measure the EC2 Network performance

1. (1 mark) The metrics of network performance include **TCP bandwidth** and **round-trip time (RTT)**. Within the same region, what network performance is experienced between instances of the same type and different types? In order to answer this question, you need to complete the following table.  

    | Type          | TCP b/w (Mbps) | RTT (ms) |
    |---------------|----------------|----------|
    | `t3.medium`-`t3.medium` |                |          |
    | `m4.large`-`m4.large`  |                |          |
    | `m5.large`-`m5.large` |                |          |
    | `t3.medium`-`m5.large`   |                |          |
    | `m4.large`-`m5.large`  |                |          |
    | `m4.large`-`t3.medium` |                |          |

    > Region: US East (N. Virginia)

2. (1 mark) What about the network performance for instances deployed in different regions? In order to answer this question, you need to complete the following table.

    | Connection | TCP b/w (Mbps)  | RTT (ms) |
    |------------|-----------------|--------------------|
    | N. Virginia-Tokyo |                 |                    |
    | N. Virginia-N. Virginia  |                 |                    |
    | Tokyo-Tokyo |                 |                    |
 
    > All instances are `m5.large`.
 
3. (1 mark) Is the network performance consistent over time? In order to answer this question, you need to complete the following table.

    | Time | TCP b/w (Mbps)  | RTT (ms) |
    |------------|-----------------|--------------------|
    | Morning |                 |                    |
    | Afternoon  |                 |                    |
    | Evening |                 |                    |
 
    > Region: US East (N. Virginia); Connection: `m5.large`-`m5.large`
 
4. (1 mark) *Open-end question:* In the above three sub-questions, you have measured network performance in different scenarios. Observe the values in each table, and try to explain why the network performance varies in different scenarios?

    > Your answer goes here.
