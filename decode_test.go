// nolint: scopelint, lll
package decoder

import (
	"encoding/json"
	"io/ioutil"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

type bucketEntry struct {
	Bucket struct {
		Name     string `json:"name,omitempty"`
		Marker   string `json:"marker,omitempty"`
		BucketID string `json:"bucket_id,omitempty"`
	} `json:"bucket,omitempty"`
	Size        uint64 `json:"size,omitempty"`
	SizeRounded uint64 `json:"size_rounded,omitempty"`
	Count       uint64 `json:"count,omitempty"`
}

func cephDecoderBucketEntry(file string) *bucketEntry {
	cmd := exec.Command("ceph-dencoder", "type", "cls_user_bucket_entry", "import", file, "decode", "dump_json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}
	var u bucketEntry
	if err = json.Unmarshal(output, &u); err != nil {
		panic(err)
	}
	return &u
}

func TestDecodeUserBucketEntry(t *testing.T) {
	testcases := []struct {
		file string
	}{
		{
			file: "testdata/bucket_entry_1",
		},
	}
	for _, tt := range testcases {
		expectedBucketEntry := cephDecoderBucketEntry(tt.file)

		data, err := ioutil.ReadFile(tt.file)
		assert.NoError(t, err)

		bucketEntry, err := DecodeUserBucketEntry(data)
		assert.NoError(t, err)

		assert.Equal(t, expectedBucketEntry.Bucket.Marker, bucketEntry.Bucket.Marker)
		assert.Equal(t, expectedBucketEntry.Bucket.BucketID, bucketEntry.Bucket.BucketID)
	}
}

type manifest struct {
	ObjSize     uint64 `json:"obj_size,omitempty"`
	HeadSize    uint64 `json:"head_size,omitempty"`
	MaxHeapSize uint64 `json:"max_heap_size,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
	Rules       []struct {
		Key uint64 `json:"key,omitempty"`
		Val struct {
			StartPartNum   uint32 `json:"start_part_num,omitempty"`
			StartOfs       uint64 `json:"start_ofs,omitempty"`
			PartSize       uint64 `json:"part_size,omitempty"`
			StripeMaxSize  uint64 `json:"stripe_max_size,omitempty"`
			OverridePrefix string `json:"override_prefix,omitempty"`
		} `json:"val,omitempty"`
	} `json:"rules,omitempty"`
	BeginIter iter `json:"begin_iter,omitempty"`
	EndIter   iter `json:"end_iter,omitempty"`
}

type iter struct {
	PartOfs           uint64 `json:"part_ofs,omitempty"`
	StripeOfs         uint64 `json:"stripe_ofs,omitempty"`
	Ofs               uint64 `json:"ofs,omitempty"`
	StripeSize        uint64 `json:"stripe_size,omitempty"`
	CurPartID         int32  `json:"cur_part_id,omitempty"`
	CurStripe         int32  `json:"cur_stripe,omitempty"`
	CurOverridePrefix string `json:"cur_override_prefix,omitempty"`
	Location          struct {
		Obj struct {
			Key struct {
				Name     string `json:"name,omitempty"`
				Instance string `json:"instance,omitempty"`
				NS       string `json:"ns,omitempty"`
			} `json:"key,omitempty"`
		} `json:"obj,omitempty"`
	} `json:"location,omitempty"`
}

func cephDecoderManifest(file string) *manifest {
	cmd := exec.Command("ceph-dencoder", "type", "RGWObjManifest", "import", file, "decode", "dump_json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}
	var m manifest
	if err = json.Unmarshal(output, &m); err != nil {
		panic(err)
	}
	return &m
}

func TestDecodeRGWObjManifest(t *testing.T) {
	testcases := []struct {
		file      string
		radosKeys []string
	}{
		{
			file: "testdata/manifest_1",
			radosKeys: []string{
				"d29b7d7b-87c7-480e-b614-3006673b3f18.56565235.42459__multipart_9c9fa8e1dda44c0c97f3e712b3d5f30d.jpg.2~Pmszle3PRpb_Olh6RT147s5stV24lCA.1",
			},
		},
		{
			file: "testdata/manifest_2",
			radosKeys: []string{
				"d29b7d7b-87c7-480e-b614-3006673b3f18.56565235.42459__multipart_76c96a4fbd8848f0864f6bb96af0595e.jpg.2~_VUeDAexPRe_u1LcyWcIgFIHX0aAAv6.1",
				"d29b7d7b-87c7-480e-b614-3006673b3f18.56565235.42459__shadow_76c96a4fbd8848f0864f6bb96af0595e.jpg.2~_VUeDAexPRe_u1LcyWcIgFIHX0aAAv6.1_1",
				"d29b7d7b-87c7-480e-b614-3006673b3f18.56565235.42459__multipart_76c96a4fbd8848f0864f6bb96af0595e.jpg.2~_VUeDAexPRe_u1LcyWcIgFIHX0aAAv6.2",
			},
		},
		{
			file: "testdata/manifest_3",
			radosKeys: []string{
				"d29b7d7b-87c7-480e-b614-3006673b3f18.55934546.831_0003b15d6914dcb1fb1622cb2b77ead2",
			},
		},
		{
			file: "testdata/manifest_4",
			radosKeys: []string{
				"d29b7d7b-87c7-480e-b614-3006673b3f18.55663851.8_174952c9-3670-4159-b1f8-f7ac56c8a817.m3u8",
			},
		},
	}
	for _, tt := range testcases {
		t.Run(tt.file, func(t *testing.T) {
			expectedManifest := cephDecoderManifest(tt.file)

			data, err := ioutil.ReadFile(tt.file)
			assert.NoError(t, err)

			manifest, err := DecodeRGWObjManifest(data)
			assert.NoError(t, err)

			assert.Equal(t, expectedManifest.ObjSize, manifest.ObjSize)
			assert.Equal(t, expectedManifest.Prefix, manifest.Prefix)
			assert.Equal(t, len(expectedManifest.Rules), len(manifest.Rules))

			for _, rule := range expectedManifest.Rules {
				assert.Equal(t, rule.Val.StartPartNum, manifest.Rules[rule.Key].StartPartNum)
				assert.Equal(t, rule.Val.StartOfs, manifest.Rules[rule.Key].StartOfs)
				assert.Equal(t, rule.Val.PartSize, manifest.Rules[rule.Key].PartSize)
				assert.Equal(t, rule.Val.StripeMaxSize, manifest.Rules[rule.Key].StripeMaxSize)
			}

			assert.Equal(t, expectedManifest.BeginIter.PartOfs, manifest.BeginIter.PartOfs)
			assert.Equal(t, expectedManifest.BeginIter.StripeOfs, manifest.BeginIter.StripeOfs)
			assert.Equal(t, expectedManifest.BeginIter.Ofs, manifest.BeginIter.Ofs)
			assert.Equal(t, expectedManifest.BeginIter.StripeSize, manifest.BeginIter.StripeSize)
			assert.Equal(t, expectedManifest.BeginIter.CurPartID, manifest.BeginIter.CurPartID)
			assert.Equal(t, expectedManifest.BeginIter.CurStripe, manifest.BeginIter.CurStripe)
			assert.Equal(t, expectedManifest.BeginIter.Location.Obj.Key.Name, manifest.BeginIter.Location.Obj.Key.Name)
			assert.Equal(t, expectedManifest.BeginIter.Location.Obj.Key.Instance, manifest.BeginIter.Location.Obj.Key.Instance)
			assert.Equal(t, expectedManifest.BeginIter.Location.Obj.Key.NS, manifest.BeginIter.Location.Obj.Key.NS)

			assert.Equal(t, expectedManifest.EndIter.PartOfs, manifest.EndIter.PartOfs)
			assert.Equal(t, expectedManifest.EndIter.StripeOfs, manifest.EndIter.StripeOfs)
			assert.Equal(t, expectedManifest.EndIter.Ofs, manifest.EndIter.Ofs)
			assert.Equal(t, expectedManifest.EndIter.StripeSize, manifest.EndIter.StripeSize)
			assert.Equal(t, expectedManifest.EndIter.CurPartID, manifest.EndIter.CurPartID)
			assert.Equal(t, expectedManifest.EndIter.CurStripe, manifest.EndIter.CurStripe)
			assert.Equal(t, expectedManifest.EndIter.Location.Obj.Key.Name, manifest.EndIter.Location.Obj.Key.Name)
			assert.Equal(t, expectedManifest.EndIter.Location.Obj.Key.Instance, manifest.EndIter.Location.Obj.Key.Instance)
			assert.Equal(t, expectedManifest.EndIter.Location.Obj.Key.NS, manifest.EndIter.Location.Obj.Key.NS)

			radosKeys := manifest.RadosObjectsKeys()
			assert.Equal(t, len(tt.radosKeys), len(radosKeys))
			for i, k := range tt.radosKeys {
				assert.Equal(t, k, radosKeys[i])
			}
		})
	}
}
