package snapshot_agent

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/Lucretius/vault_raft_snapshot_agent/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// CreateS3Snapshot writes snapshot to s3 location
func (s *Snapshotter) CreateS3Snapshot(reader io.ReadWriter, config *config.Configuration, currentTs int64) (string, error) {
	keyPrefix := "raft_snapshots"
	if config.AWS.KeyPrefix != "" {
		keyPrefix = config.AWS.KeyPrefix
	}

	input := &s3manager.UploadInput{
		Bucket:               &config.AWS.Bucket,
		Key:                  aws.String(fmt.Sprintf("%s/raft_snapshot-%d.snap", keyPrefix, currentTs)),
		Body:                 reader,
		ServerSideEncryption: nil,
	}

	if config.AWS.SSE == true {
		input.ServerSideEncryption = aws.String("AES256")
	}

	if config.AWS.StaticSnapshotName != "" {
		input.Key = aws.String(fmt.Sprintf("%s/%s.snap", keyPrefix, config.AWS.StaticSnapshotName))
	}

	o, err := s.Uploader.Upload(input)
	if err != nil {
		return "", err
	} else {
		if config.Retain > 0 && config.AWS.StaticSnapshotName == "" {
			existingSnapshotList, err := s.S3Client.ListObjects(&s3.ListObjectsInput{
				Bucket: &config.AWS.Bucket,
				Prefix: aws.String(keyPrefix),
			})
			if err != nil {
				log.Println("Error when retrieving existing snapshots for delete action.")
				return o.Location, err
			}
			existingSnapshots := make([]s3.Object, 0)

			for _, obj := range existingSnapshotList.Contents {
				if strings.HasSuffix(*obj.Key, ".snap") && strings.Contains(*obj.Key, "raft_snapshot-") {
					existingSnapshots = append(existingSnapshots, *obj)
				}
			}

			if len(existingSnapshots) <= int(config.Retain) {
				return o.Location, nil
			}

			timestamp := func(o1, o2 *s3.Object) bool {
				return o1.LastModified.Before(*o2.LastModified)
			}
			S3By(timestamp).Sort(existingSnapshots)
			if len(existingSnapshots)-int(config.Retain) <= 0 {
				return o.Location, nil
			}
			snapshotsToDelete := existingSnapshots[0 : len(existingSnapshots)-int(config.Retain)]

			for i := range snapshotsToDelete {
				_, err := s.S3Client.DeleteObject(&s3.DeleteObjectInput{
					Bucket: &config.AWS.Bucket,
					Key:    snapshotsToDelete[i].Key,
				})
				if err != nil {
					log.Printf("Error when deleting snapshot %s\n.", *snapshotsToDelete[i].Key)
					return o.Location, err
				}
			}
		}
		return o.Location, nil
	}
}

func (s *Snapshotter) HQCreateS3Snapshot(buf *bytes.Buffer, config *config.Configuration, currentTs int64) (string, error) {
	// create new tmp file and clean up old tmp file
	fileInfo, err := ioutil.ReadDir(config.Local.Path)
	if err != nil {
		log.Println("Unable to read file directory to delete old snapshots")
		return "", err
	}
	filesToDelete := make([]os.FileInfo, 0)
	for _, file := range fileInfo {
		if strings.Contains(file.Name(), "raft_snapshot-") && strings.HasSuffix(file.Name(), ".snap") {
			filesToDelete = append(filesToDelete, file)
		}
	}
	for _, f := range filesToDelete {
		os.Remove(fmt.Sprintf("%s/%s", config.Local.Path, f.Name()))
	}

	fileName := fmt.Sprintf("%s/raft_snapshot-%d.snap", config.Local.Path, currentTs)
	writefile_err := ioutil.WriteFile(fileName, buf.Bytes(), 0644)
	if writefile_err != nil {
		return "", writefile_err
	}

	// upload to s3
	keyPrefix := "raft_snapshots"
	if config.AWS.KeyPrefix != "" {
		keyPrefix = config.AWS.KeyPrefix
	}
	file, openfile_err := os.Open(fileName)
	if openfile_err != nil {
		return "", openfile_err
	}

	input := &s3manager.UploadInput{
		Bucket:               &config.AWS.Bucket,
		Key:                  aws.String(fmt.Sprintf("%s/raft_snapshot-%d.snap", keyPrefix, currentTs)),
		Body:                 file,
		ServerSideEncryption: nil,
	}

	if config.AWS.SSE {
		input.ServerSideEncryption = aws.String("AES256")
	}

	if config.AWS.StaticSnapshotName != "" {
		input.Key = aws.String(fmt.Sprintf("%s/%s.snap", keyPrefix, config.AWS.StaticSnapshotName))
	}

	o, err := s.Uploader.Upload(input)
	if err != nil {
		return "", err
	} else {
		if config.Retain > 0 && config.AWS.StaticSnapshotName == "" {
			existingSnapshotList, err := s.S3Client.ListObjects(&s3.ListObjectsInput{
				Bucket: &config.AWS.Bucket,
				Prefix: aws.String(keyPrefix),
			})
			if err != nil {
				log.Println("Error when retrieving existing snapshots for delete action.")
				return o.Location, err
			}
			existingSnapshots := make([]s3.Object, 0)

			for _, obj := range existingSnapshotList.Contents {
				if strings.HasSuffix(*obj.Key, ".snap") && strings.Contains(*obj.Key, "raft_snapshot-") {
					existingSnapshots = append(existingSnapshots, *obj)
				}
			}

			if len(existingSnapshots) <= int(config.Retain) {
				return o.Location, nil
			}

			timestamp := func(o1, o2 *s3.Object) bool {
				return o1.LastModified.Before(*o2.LastModified)
			}
			S3By(timestamp).Sort(existingSnapshots)
			if len(existingSnapshots)-int(config.Retain) <= 0 {
				return o.Location, nil
			}
			snapshotsToDelete := existingSnapshots[0 : len(existingSnapshots)-int(config.Retain)]

			for i := range snapshotsToDelete {
				_, err := s.S3Client.DeleteObject(&s3.DeleteObjectInput{
					Bucket: &config.AWS.Bucket,
					Key:    snapshotsToDelete[i].Key,
				})
				if err != nil {
					log.Printf("Error when deleting snapshot %s\n.", *snapshotsToDelete[i].Key)
					return o.Location, err
				}
			}
		}
		return o.Location, nil
	}
}

// implementation of Sort interface for s3 objects
type S3By func(f1, f2 *s3.Object) bool

func (by S3By) Sort(objects []s3.Object) {
	fs := &s3ObjectSorter{
		objects: objects,
		by:      by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(fs)
}

type s3ObjectSorter struct {
	objects []s3.Object
	by      func(f1, f2 *s3.Object) bool // Closure used in the Less method.
}

func (s *s3ObjectSorter) Len() int {
	return len(s.objects)
}

func (s *s3ObjectSorter) Less(i, j int) bool {
	return s.by(&s.objects[i], &s.objects[j])
}

func (s *s3ObjectSorter) Swap(i, j int) {
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
}
