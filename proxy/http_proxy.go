package proxy

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"net"
	"net/http"
	"strings"
)

type StorageProxy struct {
	sess          *session.Session
	s3Client      *s3.S3
	bucketName    string
	defaultPrefix string
	headerMap     map[string]string
}

func NewStorageProxy(sess *session.Session, bucketName, defaultPrefix, headerMapping string) *StorageProxy {
	headerMap := map[string]string{}
	for _, headerMapping := range strings.Split(headerMapping, ",") {
		headerMapping := strings.Split(headerMapping, "=")
		if len(headerMapping) == 2 {
			headerMap[headerMapping[0]] = headerMapping[1]
		}
	}
	return &StorageProxy{
		sess:          sess,
		s3Client:      s3.New(sess),
		bucketName:    bucketName,
		defaultPrefix: defaultPrefix,
		headerMap:     headerMap,
	}
}

func (proxy StorageProxy) objectName(name string) string {
	return proxy.defaultPrefix + name
}

func (proxy StorageProxy) Serve(port int64) error {
	http.HandleFunc("/", proxy.handler)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

	if err == nil {
		address := listener.Addr().String()
		_ = listener.Close()
		log.Printf("Starting http cache server %s\n", address)
		return http.ListenAndServe(address, nil)
	}
	return err
}

func (proxy StorageProxy) handler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	if key[0] == '/' {
		key = key[1:]
	}
	if r.Method == "GET" {
		proxy.downloadBlob(w, key)
	} else if r.Method == "HEAD" {
		proxy.checkBlobExists(w, key)
	} else if r.Method == "POST" {
		proxy.uploadBlob(w, r, key)
	} else if r.Method == "PUT" {
		proxy.uploadBlob(w, r, key)
	}
}
func (proxy StorageProxy) downloadBlob(w http.ResponseWriter, name string) {
	getObjectInput := s3.GetObjectInput{
		Bucket: aws.String(proxy.bucketName),
		Key:    aws.String(proxy.objectName(name)),
	}
	downloader := s3manager.NewDownloader(proxy.sess, func(downloader *s3manager.Downloader) {
		downloader.Concurrency = 1 // download parts sequentially for easier streaming
	})
	_, err := downloader.Download(NewSequentialWriter(w), &getObjectInput)
	if err != nil {
		log.Printf("Failed to download cache entry %s/%s: %v\n", proxy.bucketName, proxy.objectName(name), err)
		return
	}
}

func (proxy StorageProxy) checkBlobExists(w http.ResponseWriter, name string) {
	headObjectInput := s3.HeadObjectInput{
		Bucket: aws.String(proxy.bucketName),
		Key:    aws.String(proxy.objectName(name)),
	}
	objectOutput, err := proxy.s3Client.HeadObject(&headObjectInput)
	if objectOutput == nil || err != nil {
		log.Printf("Failed to check cache entry %s/%s: %v\n", proxy.bucketName, proxy.objectName(name), err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (proxy StorageProxy) uploadBlob(w http.ResponseWriter, r *http.Request, name string) {
	uploader := s3manager.NewUploader(proxy.sess)
	metadata := map[string]*string{}
	for k, v := range r.Header {
		if newHeader, exist := proxy.headerMap[strings.ToLower(k)]; exist {
			metadata[newHeader] = &v[0]
		}
	}
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:        bufio.NewReader(r.Body),
		Bucket:      aws.String(proxy.bucketName),
		ContentType: aws.String(r.Header.Get("Content-Type")),
		Key:         aws.String(proxy.objectName(name)),
		Metadata:    metadata,
	})

	if err != nil {
		log.Printf("Failed to upload cache entry %s/%s: %v\n", proxy.bucketName, proxy.objectName(name), err)
		w.WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("Failed write cache body! %s", err)
		_, _ = w.Write([]byte(errorMsg))
		return
	}
	w.WriteHeader(http.StatusCreated)
}
