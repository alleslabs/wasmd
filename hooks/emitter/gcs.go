package emitter

import (
	"bytes"
	"context"
	"fmt"

	storage "google.golang.org/api/storage/v1"
)

func UploadFile(bucket, objectPath string, message []byte) error {
	ctx := context.Background()
	client, err := storage.NewService(ctx)
	if err != nil {
		return fmt.Errorf("cannot create new GCS client: %w", err)
	}

	_, err = client.Objects.Insert(bucket, &storage.Object{Name: objectPath, ContentType: "application/json"}).Media(bytes.NewReader(message)).Do()
	if err != nil {
		return fmt.Errorf("cannot initiate write to GCS bucket: %w", err)
	}

	return nil
}
