package decoder

import (
	"errors"
	"fmt"
	"log"
)

type UserBucket struct {
	Name              string
	Marker            string
	BucketID          string
	PlacementID       string
	ExplicitPlacement explicitPlacement
}

type explicitPlacement struct {
	DataPool      string
	IndexPool     string
	DataExtraPool string
}

func DecodeUserBucket(data []byte) (*UserBucket, error) {
	d := &decoder{
		Data: data,
	}
	return d.decodeUserBucket()
}

func DecodeUserBucketEntry(data []byte) (*UserBucketEntry, error) {
	d := &decoder{
		Data: data,
	}
	return d.decodeUserBucketEntry()
}

func (d *decoder) decodeUserBucket() (*UserBucket, error) {
	var u UserBucket
	structV, structEnd, err := d.decodeStartLegacyCompatLen(8, 3, 3)
	if err != nil {
		return nil, err
	}
	name, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	u.Name = name
	if structV < 8 {
		p, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		u.ExplicitPlacement.DataPool = p
	}
	if structV >= 2 {
		marker, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		u.Marker = marker
		if structV <= 3 {
			id, err := d.decodeU64()
			if err != nil {
				return nil, err
			}
			u.BucketID = fmt.Sprintf("%d", id)
		} else {
			id, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			u.BucketID = id
		}

	}
	if structV < 8 {
		if structV >= 5 {
			p, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			u.ExplicitPlacement.IndexPool = p
		} else {
			u.ExplicitPlacement.IndexPool = u.ExplicitPlacement.DataPool
		}
	} else {
		pid, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		if structV == 8 && pid != "" {
			dp, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			ip, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			dep, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			u.ExplicitPlacement.DataPool = dp
			u.ExplicitPlacement.IndexPool = ip
			u.ExplicitPlacement.DataExtraPool = dep
		}
	}
	if structEnd > 0 {
		if d.Offset > structEnd {
			return nil, errors.New("DECODE_ERR_PAST")
		}
	}
	return &u, nil
}

type UserBucketEntry struct {
	Size           uint64
	SizeRounded    uint64
	Count          uint64
	UserStatusSync bool
	Bucket         UserBucket
}

func (d *decoder) decodeUserBucketEntry() (*UserBucketEntry, error) {
	var u UserBucketEntry
	structV, structEnd, err := d.decodeStartLegacyCompatLen(9, 5, 5)
	if err != nil {
		return nil, err
	}
	s, err := d.decodeString()
	if err != nil || s != "" {
		return nil, fmt.Errorf("notcompat or :%v", err)
	}
	size, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	u.SizeRounded = size
	u.Size = size
	if _, err = d.decodeU32(); err != nil {
		return nil, err
	}
	if structV < 7 {
		log.Println("version low")
	}
	if structV >= 2 {
		count, err := d.decodeU64()
		if err != nil {
			return nil, err
		}
		u.Count = count
	}
	if structV >= 3 {
		bucket, err := d.decodeUserBucket()
		if err != nil {
			return nil, err
		}
		u.Bucket = *bucket
	}
	if structV >= 4 {
		size, err := d.decodeU64()
		if err != nil {
			return nil, err
		}
		u.SizeRounded = size
	}
	if structV >= 6 {
		userst := d.decodeU8()
		u.UserStatusSync = userst != 0
	}
	return &u, d.decodeFinish(structEnd)
}
