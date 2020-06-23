// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Global variables
var access_key, secret_key, url_host, bucket, region string
var clean_bucket, put_object bool
var duration_secs, threads, loops, num_objs, start_idx int
var object_size uint64
var running_threads, upload_count, delete_count, upload_slowdown_count int32
var download_count, obj_idx, download_slowdown_count, delete_slowdown_count int32
var endtime, upload_finish, download_finish, delete_finish time.Time

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

// Our HTTP transport used for the roundtripper below
var HTTPTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0,
	// Allow an unlimited number of idle connections
	MaxIdleConnsPerHost: 4096,
	MaxIdleConns:        0,
	// But limit their idle time
	IdleConnTimeout: time.Minute,
	// Ignore TLS errors
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var httpClient = &http.Client{Transport: HTTPTransport}

func getS3Client() *s3.S3 {
	// Build our config
	creds := credentials.NewStaticCredentials(access_key, secret_key, "")
	loglevel := aws.LogOff
	// Build the rest of the configuration
	awsConfig := &aws.Config{
		Region:               aws.String(region),
		Endpoint:             aws.String(url_host),
		Credentials:          creds,
		LogLevel:             &loglevel,
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
		// Comment following to use default transport
		HTTPClient: &http.Client{Transport: HTTPTransport},
	}
	session := session.New(awsConfig)
	client := s3.New(session)
	if client == nil {
		log.Fatalf("FATAL: Unable to create new client.")
	}
	// Return success
	return client
}

func createBucket(ignore_errors bool) {
	// Get a client
	client := getS3Client()
	// Create our bucket (may already exist without error)
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if _, err := client.CreateBucket(in); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists, s3.ErrCodeBucketAlreadyOwnedByYou:
				// do nothing
				return
			default:
				if ignore_errors {
					log.Printf("WARNING: createBucket %s error, ignoring %v", bucket, err)
				} else {
					log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v", bucket, err)
				}
			}
		}
	}
}

func deleteAllObjects() {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	var doneDeletes sync.WaitGroup
	// Loop deleting our versions reading as big a list as we can
	var err error
	// Delete all the existing objects and versions in the bucket
	in := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(1000),
	}
	result, err := client.ListObjects(in)
	if err != nil {
		fmt.Printf("Error listing bucket:\n%v\n", err)
	}
	for _, object := range result.Contents {
		doDelete := func(bucket string, key string) {
			_, err = client.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
				}
			} else {
				fmt.Printf("Deleted: %s/%s\n", bucket, key)
			}
			doneDeletes.Done()
		}
		doneDeletes.Add(1)
		go doDelete(bucket, *object.Key)
	}
	// Wait for deletes to finish
	doneDeletes.Wait()
	// If error, it is fatal
	if err != nil {
		log.Fatalf("FATAL: Unable to delete objects from bucket: %v", err)
	}
}

// canonicalAmzHeaders -- return the x-amz headers canonicalized
func canonicalAmzHeaders(req *http.Request) string {
	// Parse out all x-amz headers
	var headers []string
	for header := range req.Header {
		norm := strings.ToLower(strings.TrimSpace(header))
		if strings.HasPrefix(norm, "x-amz") {
			headers = append(headers, norm)
		}
	}
	// Put them in sorted order
	sort.Strings(headers)
	// Now add back the values
	for n, header := range headers {
		headers[n] = header + ":" + strings.Replace(req.Header.Get(header), "\n", " ", -1)
	}
	// Finally, put them back together
	if len(headers) > 0 {
		return strings.Join(headers, "\n") + "\n"
	} else {
		return ""
	}
}

func hmacSHA1(key []byte, content string) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(content))
	return mac.Sum(nil)
}

func setSignature(req *http.Request) {
	// Setup default parameters
	dateHdr := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("X-Amz-Date", dateHdr)
	// Get the canonical resource and header
	canonicalResource := req.URL.EscapedPath()
	canonicalHeaders := canonicalAmzHeaders(req)
	stringToSign := req.Method + "\n" + req.Header.Get("Content-MD5") + "\n" + req.Header.Get("Content-Type") + "\n\n" +
		canonicalHeaders + canonicalResource
	hash := hmacSHA1([]byte(secret_key), stringToSign)
	signature := base64.StdEncoding.EncodeToString(hash)
	req.Header.Set("Authorization", fmt.Sprintf("AWS %s:%s", access_key, signature))
}

func runUpload(thread_num int) {
	// Get a client
	client := getS3Client()

	for time.Now().Before(endtime) {
		objnum := atomic.AddInt32(&upload_count, 1)
		// Initialize data for the object
		object_data := make([]byte, object_size)
		rand.Read(object_data)
		fileobj := bytes.NewReader(object_data)
		key := fmt.Sprintf("Object-%d", objnum)
		mgr := s3manager.NewUploaderWithClient(client)
		upParams := &s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   fileobj,
		}
		_, err := mgr.Upload(upParams, func(u *s3manager.Uploader) {
			u.PartSize = 200 * 1024 * 1024 // 200MB part size
		})
		object_data = nil
		fileobj = nil
		mgr = nil
		upParams = nil
		if err != nil {
			fmt.Printf("Failed to upload data to %s/%s, %s\n", bucket, key, err.Error())
			atomic.AddInt32(&upload_slowdown_count, 1)
			atomic.AddInt32(&upload_count, -1)
			return
		} else {
			fmt.Printf("%d: Uploaded data to %s/%s\n", thread_num, bucket, key)
		}
	}

	// Remember last done time
	upload_finish = time.Now()

	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDownload(thread_num int) {
	// Get a client
	client := getS3Client()
	for time.Now().Before(endtime) {
		// use num_objs until change to HEAD container
		objnum := rand.Intn(num_objs) + 1
		key := fmt.Sprintf("Object-%d", objnum)
		file, err := os.Create(os.DevNull)
		mgr := s3manager.NewDownloaderWithClient(client)
		_, err = mgr.Download(file, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			fmt.Printf("Failed to download data to %s/%s, %s\n", bucket, key, err.Error())
			atomic.AddInt32(&download_slowdown_count, 1)
			return
		} else {
			atomic.AddInt32(&download_count, 1)
			fmt.Printf(".")
			//fmt.Printf("Downloaded obj %s/%s of length: %d\n", bucket, key, size)
		}
	}

	// Remember last done time
	download_finish = time.Now()

	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDownload2(thread_num int) {
	for time.Now().Before(endtime) {
		atomic.AddInt32(&download_count, 1)
		atomic.CompareAndSwapInt32(&obj_idx, int32(num_objs), 0)
		atomic.AddInt32(&obj_idx, 1)
		//objnum := rand.Intn(num_objs) + 1
		prefix := fmt.Sprintf("%s/%s/Object-%d", url_host, bucket, obj_idx)
		req, _ := http.NewRequest("GET", prefix, nil)
		setSignature(req)
		if resp, err := httpClient.Do(req); err != nil {
			log.Fatalf("FATAL: Error downloading object %s: %v", prefix, err)
		} else if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				atomic.AddInt32(&download_slowdown_count, 1)
				atomic.AddInt32(&download_count, -1)
			} else if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				atomic.AddInt32(&download_count, -1)
				fmt.Printf("Failed to download obj %s: : %d\n", prefix, resp.StatusCode)
			} else {
				io.Copy(ioutil.Discard, resp.Body)
				fmt.Printf(".")
				//fmt.Printf("Finished download obj %s of length: %d\n", prefix, l)
			}
		}
	}
	// Remember last done time
	download_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDelete(thread_num int) {
	for {
		objnum := atomic.AddInt32(&delete_count, 1)
		if objnum > upload_count {
			break
		}
		prefix := fmt.Sprintf("%s/%s/Object-%d", url_host, bucket, objnum)
		req, _ := http.NewRequest("DELETE", prefix, nil)
		setSignature(req)
		if resp, err := httpClient.Do(req); err != nil {
			log.Fatalf("FATAL: Error deleting object %s: %v", prefix, err)
		} else if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			atomic.AddInt32(&delete_slowdown_count, 1)
			atomic.AddInt32(&delete_count, -1)
		}
	}
	// Remember last done time
	delete_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func main() {
	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&access_key, "a", "", "Access key")
	myflag.StringVar(&secret_key, "s", "", "Secret key")
	myflag.StringVar(&url_host, "u", "http://s3.wasabisys.com", "URL for host with method prefix")
	myflag.StringVar(&bucket, "b", "s3-benchmark-bucket", "Bucket for testing")
	myflag.BoolVar(&clean_bucket, "c", false, "clean bucket")
	myflag.BoolVar(&put_object, "p", false, "PUT objects")
	myflag.StringVar(&region, "r", "us-east-1", "Region for testing")
	myflag.IntVar(&duration_secs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	myflag.IntVar(&num_objs, "n", 10, "Number of objects to get")
	myflag.IntVar(&start_idx, "i", 0, "Starting index")

	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	if access_key == "" {
		log.Fatal("Missing argument -a for access key.")
	}
	if secret_key == "" {
		log.Fatal("Missing argument -s for secret key.")
	}
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Echo the parameters
	logit(fmt.Sprintf("Parameters: url=%s, bucket=%s, region=%s, duration=%d, threads=%d, loops=%d, size=%s",
		url_host, bucket, region, duration_secs, threads, loops, sizeArg))

	// Create the bucket and delete all the objects
	createBucket(true)
	if clean_bucket {
		deleteAllObjects()
	}

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		upload_count = int32(start_idx)
		upload_slowdown_count = 0
		download_count = 0
		obj_idx = 0
		download_slowdown_count = 0
		delete_count = 0
		delete_slowdown_count = 0

		// Run the upload case
		if put_object {
			running_threads = int32(threads)
			starttime := time.Now()
			endtime = starttime.Add(time.Second * time.Duration(duration_secs))
			for n := 1; n <= threads; n++ {
				go runUpload(n)
			}

			// Wait for it to finish
			for atomic.LoadInt32(&running_threads) > 0 {
				time.Sleep(time.Millisecond)
			}
			upload_time := upload_finish.Sub(starttime).Seconds()

			bps := float64(uint64(upload_count)*object_size) / upload_time
			logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
				loop, upload_time, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_count)/upload_time, upload_slowdown_count))
			num_objs = int(upload_count)
		}

		// Run the download case
		running_threads = int32(threads)
		get_starttime := time.Now()
		endtime = get_starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDownload2(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		download_time := download_finish.Sub(get_starttime).Seconds()

		get_bps := float64(uint64(download_count)*object_size) / download_time
		logit(fmt.Sprintf("\nLoop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			loop, download_time, download_count, bytefmt.ByteSize(uint64(get_bps)), float64(download_count)/download_time, download_slowdown_count))

		// Run the delete case
		/*
			running_threads = int32(threads)
			starttime = time.Now()
			endtime = starttime.Add(time.Second * time.Duration(duration_secs))
			for n := 1; n <= threads; n++ {
				go runDelete(n)
			}

			// Wait for it to finish
			for atomic.LoadInt32(&running_threads) > 0 {
				time.Sleep(time.Millisecond)
			}
			delete_time := delete_finish.Sub(starttime).Seconds()

			logit(fmt.Sprintf("Loop %d: DELETE time %.1f secs, %.1f deletes/sec. Slowdowns = %d",
				loop, delete_time, float64(upload_count)/delete_time, delete_slowdown_count))
		*/
	}

	// All done
}
