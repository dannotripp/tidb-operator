// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"errors"
	"testing"

	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestIsObjectStreamEmpty(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		objects := make(chan minio.ObjectInfo)
		close(objects)

		empty, err := isObjectStreamEmpty(objects)
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("asynchronous object", func(t *testing.T) {
		objects := make(chan minio.ObjectInfo)
		go func() {
			objects <- minio.ObjectInfo{Key: "backup/meta"}
			close(objects)
		}()

		empty, err := isObjectStreamEmpty(objects)
		require.NoError(t, err)
		require.False(t, empty)
	})

	t.Run("listing error", func(t *testing.T) {
		listErr := errors.New("list failed")
		objects := make(chan minio.ObjectInfo, 1)
		objects <- minio.ObjectInfo{Err: listErr}
		close(objects)

		empty, err := isObjectStreamEmpty(objects)
		require.ErrorIs(t, err, listErr)
		require.False(t, empty)
	})
}

func TestMinioCredentials(t *testing.T) {
	t.Run("raw Secret data", func(t *testing.T) {
		secret := &corev1.Secret{Data: map[string][]byte{
			"access_key": []byte("12345678"),
			"secret_key": []byte("abcdefgh"),
		}}

		accessKey, secretKey, err := minioCredentials(secret)
		require.NoError(t, err)
		require.Equal(t, "12345678", accessKey)
		require.Equal(t, "abcdefgh", secretKey)
	})

	t.Run("missing key", func(t *testing.T) {
		secret := &corev1.Secret{Data: map[string][]byte{
			"access_key": []byte("12345678"),
		}}

		_, _, err := minioCredentials(secret)
		require.EqualError(t, err, "access_key or secret_key not found")
	})
}
