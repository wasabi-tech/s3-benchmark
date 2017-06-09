# s3-benchmark
s3-benchmark is a program for performing S3 operations PUT, GET, and DELETE for objects.  Besides the 
bucket configuration, the object size and number of threads can be given for different tests.  

The test is loosely based on the Nasuni benchmark used to test the performance of different cloud
storage providers.

# Building the Program
If the test is being run on the Ubuntu version 16.04 LTS (the current long term release), the binary
executable s3-benchmark.ubuntu will run the benchmark without building. 

Otherwise, to build the test, you must install the Go 1.7 system along with the supporting libraries.
 
# Command Line Arguments
Below are the command line arguments to the program:

  -a string
        Access key
  -b string
        Bucket for testing (default "wasabi-benchmark-bucket")
  -d int
        Duration of each test in seconds (default 60)
  -l int
        Number of times to repeat test (default 1)
  -s string
        Secret key
  -t int
        Number of threads to run (default 1)
  -u string
        URL for host with method prefix (default "http://s3.wasabisys.com")
  -z string
        Size of objects in bytes with postfix K, M, and G (default "1M")

# Example Benchmark

