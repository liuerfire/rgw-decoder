package decoder

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	shadowNS  = "shadow"
	multipart = "multipart"
)

type RGWObjManifest struct {
	ExplicitObjs      bool
	ObjSize           uint64
	HeadSize          uint64
	MaxHeapSize       uint64
	Prefix            string
	TailInstance      string
	Objs              map[uint64]RGWObjManifestPart
	Rules             ruleIterator
	TailPlacement     RGWBucketPlacement
	BeginIter         ObjIterator
	EndIter           ObjIterator
	Obj               RGWObj
	HeadPlacementRule RGWPlacementRule
}

type RGWObjManifestPart struct {
	LocOfs uint64
	Size   uint64
	Loc    RGWObj
}

type ruleIterator map[uint64]RGWObjManifestRule

type rulePair struct {
	end    bool
	First  uint64
	Second RGWObjManifestRule
}

type ObjIterator struct {
	manifest          *RGWObjManifest
	PartOfs           uint64
	StripeOfs         uint64
	Ofs               uint64
	StripeSize        uint64
	CurPartID         int32
	CurStripe         int32
	CurOverridePrefix string
	Location          RGWObjSelect
	RuleIter          rulePair
	NextRuleIter      rulePair
}

type RGWObjSelect struct {
	PlacementRule RGWPlacementRule
	IsRaw         bool
	Obj           RGWObj
	RawObj        RGWRawObj
}

type RGWObjManifestRule struct {
	StartPartNum   uint32
	StartOfs       uint64
	PartSize       uint64
	StripeMaxSize  uint64
	OverridePrefix string
}

type RGWBucketPlacement struct {
	PlacementRule RGWPlacementRule
	Bucket        RGWBucket
}

type RGWPlacementRule struct {
	Name         string
	StorageClass string
}

type RGWRawObj struct {
	Oid  string
	Loc  string
	Pool RGWPool
}

type RGWObj struct {
	Bucket      RGWBucket
	Key         RGWObjKey
	InExtraData bool
}

type RGWBucket struct {
	Tenant            string
	Name              string
	Marker            string
	BucketID          string
	ExplicitPlacement RGWDataPlacementTarget
}

type RGWDataPlacementTarget struct {
	DataPool      RGWPool
	DataExtraPool RGWPool
	IndexPool     RGWPool
}

type RGWPool struct {
	Name string
	NS   string
}

type RGWObjKey struct {
	Name     string
	NS       string
	Instance string
}

func DecodeRGWObjManifest(data []byte) (*RGWObjManifest, error) {
	d := &decoder{
		Data: data,
	}
	return d.decodeRGWObjManifest()
}

func initObjIterator(r *RGWObjManifest) *ObjIterator {
	iter := &ObjIterator{
		manifest: r,
	}
	iter.seek(0)
	return iter
}

func (o *ObjIterator) seek(ofs uint64) {
	o.Ofs = ofs
	if ofs < o.manifest.HeadSize {
		o.RuleIter = o.manifest.Rules.begin()
		o.StripeOfs = 0
		o.StripeSize = o.manifest.HeadSize
		if !o.RuleIter.equal(o.manifest.Rules.end()) {
			o.CurPartID = int32(o.RuleIter.Second.StartPartNum)
			o.CurOverridePrefix = o.RuleIter.Second.OverridePrefix
		}
		o.updataLocation()
		return
	}
	o.RuleIter = o.manifest.Rules.upperBound(ofs)
	o.NextRuleIter = o.RuleIter

	if !o.RuleIter.equal(o.manifest.Rules.begin()) {
		o.manifest.Rules.backward(&o.RuleIter)
	}

	if o.RuleIter.equal(o.manifest.Rules.end()) {
		o.updataLocation()
		return
	}

	rule := &o.RuleIter.Second

	if rule.PartSize > 0 {
		o.CurPartID = int32(uint64(rule.StartPartNum) + (o.Ofs-rule.StartOfs)/rule.PartSize)
	} else {
		o.CurPartID = int32(rule.StartPartNum)
	}
	o.PartOfs = rule.StartOfs + uint64(uint32(o.CurPartID)-rule.StartPartNum)*rule.PartSize
	if rule.StripeMaxSize > 0 {
		o.CurStripe = int32((o.Ofs - o.PartOfs) / rule.StripeMaxSize)
		o.StripeOfs = o.PartOfs + uint64(o.CurStripe)*rule.StripeMaxSize
		if o.CurPartID == 0 && o.manifest.HeadSize > 0 {
			o.CurStripe++
		}
	} else {
		o.CurStripe = 0
		o.StripeOfs = o.PartOfs
	}

	if rule.PartSize == 0 {
		o.StripeSize = rule.StripeMaxSize
		o.StripeSize = minuint64(o.manifest.ObjSize-o.StripeOfs, o.StripeSize)
	} else {
		next := minuint64(o.StripeOfs+rule.StripeMaxSize, o.PartOfs+rule.PartSize)
		o.StripeSize = next - o.StripeOfs
	}
	o.CurOverridePrefix = rule.OverridePrefix
	o.updataLocation()
}

func (o *ObjIterator) equal(i *ObjIterator) bool {
	return o.Ofs == i.Ofs
}

func (o *ObjIterator) updataLocation() {
	if o.Ofs < o.manifest.HeadSize {
		o.Location = RGWObjSelect{
			Obj: o.manifest.Obj,
		}
		o.Location.PlacementRule = o.manifest.HeadPlacementRule
		return
	}
	o.manifest.getImplicitLocation(o.CurPartID, o.CurStripe, o.Ofs, o.CurOverridePrefix, &o.Location)
}

func (o *ObjIterator) iterate() {
	objSize := o.manifest.ObjSize
	headSize := o.manifest.HeadSize

	if objSize == o.Ofs {
		return
	}
	if len(o.manifest.Rules) < 1 {
		return
	}

	if o.Ofs < headSize {
		o.RuleIter = o.manifest.Rules.begin()
		rule := &o.RuleIter.Second
		o.Ofs = minuint64(headSize, objSize)
		o.StripeOfs = o.Ofs
		o.CurStripe = 1
		o.StripeSize = minuint64(objSize-o.Ofs, rule.StripeMaxSize)
		if rule.PartSize > 0 {
			o.StripeSize = minuint64(o.StripeSize, rule.PartSize)
		}
		o.updataLocation()
		return
	}

	rule := &o.RuleIter.Second

	o.StripeOfs += rule.StripeMaxSize
	o.CurStripe++

	if rule.PartSize > 0 {
		if o.StripeOfs >= o.PartOfs+rule.PartSize {
			o.CurStripe = 0
			o.PartOfs += rule.PartSize
			o.StripeOfs = o.PartOfs
			lastRule := o.NextRuleIter.equal(o.manifest.Rules.end())
			if !lastRule && o.StripeOfs >= o.NextRuleIter.Second.StartOfs {
				o.RuleIter = o.NextRuleIter
				if !lastRule {
					o.manifest.Rules.forward(&o.NextRuleIter)
				}
				o.CurPartID = int32(o.RuleIter.Second.StartPartNum)
			} else {
				o.CurPartID++
			}
			rule = &o.RuleIter.Second
		}
		o.StripeSize = minuint64(rule.PartSize-(o.StripeOfs-o.PartOfs), rule.StripeMaxSize)
	}

	o.CurOverridePrefix = rule.OverridePrefix
	o.Ofs = o.StripeOfs
	if o.Ofs > objSize {
		o.Ofs = objSize
		o.StripeOfs = o.Ofs
		o.StripeSize = 0
	}
	o.updataLocation()
}

func (r ruleIterator) begin() rulePair {
	var keys []uint64
	for k := range r {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return rulePair{
		First:  keys[0],
		Second: r[keys[0]],
	}
}

func (r ruleIterator) end() rulePair {
	return rulePair{
		end: true,
	}
}

func (r ruleIterator) backward(p *rulePair) {
	var keys []uint64

	for k := range r {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	end := len(keys) - 1

	var pos int
	for i, k := range keys {
		if k == p.First {
			pos = i - 1
			break
		}
	}
	if pos < 0 || p.end {
		*p = rulePair{
			First:  keys[end],
			Second: r[keys[end]],
		}
		return
	}
	*p = rulePair{
		First:  keys[pos],
		Second: r[keys[pos]],
	}
}

func (r ruleIterator) forward(p *rulePair) {
	var keys []uint64
	for k := range r {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	var pos int
	for i, k := range keys {
		if k == p.First {
			pos = i + 1
		}
	}
	if pos >= len(keys) || p.end {
		*p = rulePair{
			end: true,
		}
		return
	}
	*p = rulePair{
		First:  keys[pos],
		Second: r[keys[pos]],
	}
}

func (r ruleIterator) upperBound(key uint64) rulePair {
	var keys []uint64
	for k := range r {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for i, k := range keys {
		if key < k {
			return rulePair{
				First:  keys[i],
				Second: r[keys[i]],
			}
		}
	}
	return rulePair{
		end: true,
	}
}

func (r rulePair) equal(i rulePair) bool {
	if r.end {
		return i.end
	}
	if i.end {
		return false
	}
	return r.First == i.First
}

func (r *RGWObjManifest) RadosObjectsKeys() []string {
	var keys []string
	for !r.EndIter.equal(&r.BeginIter) {
		var key string
		if r.BeginIter.Location.Obj.Key.NS != "" {
			key = fmt.Sprintf("%s__%s_%s", r.BeginIter.Location.Obj.Bucket.Marker,
				r.BeginIter.Location.Obj.Key.NS, r.BeginIter.Location.Obj.Key.Name)
		} else {
			key = fmt.Sprintf("%s_%s", r.BeginIter.Location.Obj.Bucket.Marker,
				r.BeginIter.Location.Obj.Key.Name)
		}
		keys = append(keys, key)
		r.BeginIter.iterate()
	}
	return keys
}

func (r *RGWObjManifest) updateIterators() {
	r.BeginIter = *initObjIterator(r)
	r.EndIter = *initObjIterator(r)
	r.BeginIter.seek(0)
	r.EndIter.seek(r.ObjSize)
}

func (r *RGWObjManifest) getImplicitLocation(cur_part_id, cur_stripe int32, ofs uint64,
	override_prefix string, location *RGWObjSelect) {
	var loc RGWObj
	oid := &loc.Key.Name
	ns := &loc.Key.NS
	if override_prefix == "" {
		*oid = r.Prefix
	} else {
		*oid = override_prefix
	}
	if cur_part_id == 0 {
		if ofs < r.MaxHeapSize {
			location.PlacementRule = r.HeadPlacementRule
			*location = RGWObjSelect{
				Obj:   r.Obj,
				IsRaw: false,
			}
			return
		} else {
			buf := fmt.Sprintf("%d", int(cur_stripe))
			*oid += buf
			*ns = shadowNS
		}
	} else {
		if cur_stripe == 0 {
			buf := fmt.Sprintf(".%d", int(cur_part_id))
			*oid += buf
			*ns = multipart
		} else {
			buf := fmt.Sprintf(".%d_%d", int(cur_part_id), int(cur_stripe))
			*oid += buf
			*ns = shadowNS
		}
	}
	if r.TailPlacement.Bucket.Name != "" {
		loc.Bucket = r.TailPlacement.Bucket
	} else {
		loc.Bucket = r.Obj.Bucket
	}
	loc.Key.Instance = r.TailInstance

	location.PlacementRule = r.TailPlacement.PlacementRule
	*location = RGWObjSelect{
		Obj:   loc,
		IsRaw: false,
	}
}

func (r *RGWObj) getOID() string {
	return r.Key.getOID()
}

func (r *RGWObjKey) getOID() string {
	if r.NS != "" && !r.needToEncodeInstance() {
		if len(r.Name) < 1 || r.Name[0] != '_' {
			return r.Name
		}
		return "_" + r.Name
	}
	oid := "_"
	oid += r.NS
	if r.needToEncodeInstance() {
		oid += ":" + r.Instance
	}
	oid += "_"
	oid += r.Name
	return oid
}

func (r *RGWObjKey) needToEncodeInstance() bool {
	return r.Instance != "" && r.Instance != "null"
}

func (r *RGWPlacementRule) fromStr(s string) {
	pos := strings.Index(s, "/")
	if pos < 0 {
		r.Name = s
		r.StorageClass = ""
		return
	}
	r.Name = s[:pos]
	r.StorageClass = s[pos+1:]
}

func (d *decoder) decodeRGWPlacementRule() (*RGWPlacementRule, error) {
	var r RGWPlacementRule

	s, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.fromStr(s)
	return &r, nil
}

func (d *decoder) decodeRGWObjKey() (*RGWObjKey, error) {
	var r RGWObjKey
	structV, _, _, err := d.decodeStart(2)
	if err != nil {
		return nil, err
	}
	name, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.Name = name
	instance, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.Instance = instance
	if structV >= 2 {
		ns, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.NS = ns
	}
	return &r, nil
}

func (d *decoder) decodeRGWPool() (*RGWPool, error) {
	var r RGWPool

	_, structEnd, err := d.decodeStartLegacyCompatLen(10, 3, 3)
	if err != nil {
		return nil, err
	}
	name, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.Name = name
	ns, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.NS = ns
	return &r, d.decodeFinish(structEnd)
}

func (d *decoder) decodeRGWBucket() (*RGWBucket, error) {
	var r RGWBucket

	structV, structEnd, err := d.decodeStartLegacyCompatLen(10, 3, 3)
	if err != nil {
		return nil, err
	}
	name, err := d.decodeString()
	if err != nil {
		return nil, err
	}
	r.Name = name
	if structV < 10 {
		pname, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.ExplicitPlacement.DataPool.Name = pname
	}
	if structV >= 2 {
		mk, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Marker = mk
		if structV <= 3 {
			id, err := d.decodeU64()
			if err != nil {
				return nil, err
			}
			r.BucketID = fmt.Sprintf("%d", id)
		} else {
			bid, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			r.BucketID = bid
		}
	}
	if structV < 10 {
		if structV >= 5 {
			name, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			r.ExplicitPlacement.DataPool.Name = name
		} else {
			r.ExplicitPlacement.IndexPool = r.ExplicitPlacement.DataPool
		}
		if structV >= 7 {
			name, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			r.ExplicitPlacement.DataExtraPool.Name = name
		}
	}
	if structV >= 8 {
		tenant, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Tenant = tenant
	}
	if structV >= 10 {
		decodeExplicit := d.decodeBool()
		if decodeExplicit {
			dataPool, err := d.decodeRGWPool()
			if err != nil {
				return nil, err
			}
			r.ExplicitPlacement.DataPool = *dataPool

			extraPool, err := d.decodeRGWPool()
			if err != nil {
				return nil, err
			}
			r.ExplicitPlacement.DataExtraPool = *extraPool

			indexPool, err := d.decodeRGWPool()
			if err != nil {
				return nil, err
			}
			r.ExplicitPlacement.IndexPool = *indexPool
		}
	}

	return &r, d.decodeFinish(structEnd)
}

func (d *decoder) decodeRGWObj() (*RGWObj, error) {
	var r RGWObj
	structV, structEnd, err := d.decodeStartLegacyCompatLen(6, 3, 3)
	if err != nil {
		return nil, err
	}

	if structV < 6 {
		name, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Bucket.Name = name
		if _, err = d.decodeString(); err != nil {
			return nil, err
		}
		ns, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Key.NS = ns

		keyName, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Key.Name = keyName

		if structV >= 2 {
			bucket, err := d.decodeRGWBucket()
			if err != nil {
				return nil, err
			}
			r.Bucket = *bucket
		}
		if structV >= 4 {
			ins, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			r.Key.Instance = ins
		}
		if r.Key.NS == "" && r.Key.Instance == "" {
			r.Key.Name = strings.TrimPrefix(r.Key.Name, "_")
		} else {
			if structV >= 5 {
				name, err := d.decodeString()
				if err != nil {
					return nil, err
				}
				r.Key.Name = name
			} else {
				i := strings.Index(r.Key.Name, "_")
				if i < 0 {
					return nil, errors.New("DECODE_ERR_PAST")
				}
				r.Key.Name = r.Key.Name[i+1:]
			}
		}
	} else {
		bucket, err := d.decodeRGWBucket()
		if err != nil {
			return nil, err
		}
		r.Bucket = *bucket
		ns, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Key.NS = ns

		keyName, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Key.Name = keyName

		ins, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Key.Instance = ins
	}
	return &r, d.decodeFinish(structEnd)
}

func (d *decoder) decodeRGWObjManifestRule() (*RGWObjManifestRule, error) {
	var r RGWObjManifestRule

	structV, _, structEnd, err := d.decodeStart(2)
	if err != nil {
		return nil, err
	}

	spn, err := d.decodeU32()
	if err != nil {
		return nil, err
	}
	r.StartPartNum = spn

	startOfs, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.StartOfs = startOfs

	ps, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.PartSize = ps

	sms, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.StripeMaxSize = sms

	if structV >= 2 {
		op, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.OverridePrefix = op
	}

	return &r, d.decodeFinish(structEnd)
}

func (d *decoder) decodeRGWObjManifestPart() (*RGWObjManifestPart, error) {
	var r RGWObjManifestPart

	_, structEnd, err := d.decodeStartLegacyCompatLen(2, 2, 2)
	if err != nil {
		return nil, err
	}
	robj, err := d.decodeRGWObj()
	if err != nil {
		return nil, err
	}
	r.Loc = *robj

	locOfs, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.LocOfs = locOfs

	size, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.Size = size
	return &r, d.decodeFinish(structEnd)
}

func (d *decoder) decodeRGWObjManifest() (*RGWObjManifest, error) {
	r := RGWObjManifest{
		Objs:  make(map[uint64]RGWObjManifestPart),
		Rules: make(map[uint64]RGWObjManifestRule),
	}
	structV, structEnd, err := d.decodeStartLegacyCompatLen(7, 2, 2)
	if err != nil {
		return nil, err
	}

	objSize, err := d.decodeU64()
	if err != nil {
		return nil, err
	}
	r.ObjSize = objSize
	l, err := d.decodeU32()
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < l; i++ {
		k, err := d.decodeU64()
		if err != nil {
			return nil, err
		}
		part, err := d.decodeRGWObjManifestPart()
		if err != nil {
			return nil, err
		}
		r.Objs[k] = *part
	}
	if structV > 3 {
		r.ExplicitObjs = d.decodeBool()
		obj, err := d.decodeRGWObj()
		if err != nil {
			return nil, err
		}
		r.Obj = *obj

		hs, err := d.decodeU64()
		if err != nil {
			return nil, err
		}
		r.HeadSize = hs

		mhs, err := d.decodeU64()
		if err != nil {
			return nil, err
		}
		r.MaxHeapSize = mhs

		prefix, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		r.Prefix = prefix

		l, err := d.decodeU32()
		if err != nil {
			return nil, err
		}
		for i := uint32(0); i < l; i++ {
			k, err := d.decodeU64()
			if err != nil {
				return nil, err
			}
			rule, err := d.decodeRGWObjManifestRule()
			if err != nil {
				return nil, err
			}
			r.Rules[k] = *rule
		}
	}
	if r.ExplicitObjs && r.HeadSize > 0 && len(r.Objs) > 0 {
		obj0 := r.Objs[0].Loc
		if obj0.getOID() != "" && obj0.Key.NS != "" {
			o := r.Objs[0]
			o.Loc = r.Obj
			o.Size = r.HeadSize
			r.Objs[0] = o
		}
	}

	if structV >= 4 {
		if structV < 6 {
			bucket, err := d.decodeRGWBucket()
			if err != nil {
				return nil, err
			}
			r.TailPlacement.Bucket = *bucket
		} else {
			if d.decodeBool() {
				bucket, err := d.decodeRGWBucket()
				if err != nil {
					return nil, err
				}
				r.TailPlacement.Bucket = *bucket
			} else {
				r.TailPlacement.Bucket = r.Obj.Bucket
			}
		}
	}

	if structV >= 5 {
		if structV < 6 {
			ins, err := d.decodeString()
			if err != nil {
				return nil, err
			}
			r.TailInstance = ins
		} else {
			if d.decodeBool() {
				ins, err := d.decodeString()
				if err != nil {
					return nil, err
				}
				r.TailInstance = ins
			} else {
				r.TailInstance = r.Obj.Key.Instance
			}
		}
	} else {
		r.TailInstance = r.Obj.Key.Instance
	}
	if structV >= 7 {
		hpr, err := d.decodeRGWPlacementRule()
		if err != nil {
			return nil, err
		}
		r.HeadPlacementRule = *hpr

		tpr, err := d.decodeRGWPlacementRule()
		if err != nil {
			return nil, err
		}
		r.TailPlacement.PlacementRule = *tpr
	}
	r.updateIterators()
	return &r, d.decodeFinish(structEnd)
}

func minuint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}
