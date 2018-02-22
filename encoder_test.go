package fluxline

// DCSO fluxline
// Copyright (c) 2017, 2018, DCSO GmbH

import (
	"bytes"
	"io"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
)

var testStruct = struct {
	TestVal     uint64    `influx:"testval"`
	TestVal2    uint64    `influx:"testvalue"`
	TestVal3    int64     `influx:"testvalue2"`
	TestVal4    string    `influx:"testvalue3"`
	TestDate    time.Time `influx:"testvaluetime"`
	TestBool    bool      `influx:"testvaluebool"`
	TestFloat   float64   `influx:"testvalueflt64"`
	TestFloat32 float32   `influx:"testvalueflt32"`
}{
	TestVal:     1,
	TestVal2:    2,
	TestVal3:    -3,
	TestVal4:    `foobar"baz`,
	TestDate:    time.Now(),
	TestFloat32: math.Pi,
	TestFloat:   1.29e-24,
}

var testStructInvalidType = struct {
	TestVal  uint64    `influx:"testval"`
	TestVal2 uint64    `influx:"testvalue"`
	TestVal3 int64     `influx:"testvalue2"`
	Foo      io.Writer `influx:"testinval"`
}{
	TestVal:  1,
	TestVal2: 2,
	TestVal3: -3,
}

var testStructStringLong = struct {
	TestStr string `influx:"testval"`
}{
	TestStr: strings.Repeat("#", 70000),
}

var testStructPartUntagged = struct {
	TestVal  uint64 `influx:"testval"`
	TestVal2 uint64 `influx:"testvalue"`
	TestVal3 int64
}{
	TestVal:  1,
	TestVal2: 2,
	TestVal3: -3,
}

var testStructAllUntagged = struct {
	TestVal  uint64
	TestVal2 uint64
	TestVal3 int64
}{
	TestVal:  1,
	TestVal2: 2,
	TestVal3: -3,
}

func TestEncoderEncoder(t *testing.T) {
	var b bytes.Buffer

	ile := NewEncoder(&b)
	tags := make(map[string]string)
	tags["foo"] = "bar"
	tags["baaz gogo"] = "gu,gu"
	err := ile.Encode("mytool", testStruct, tags)
	if err != nil {
		t.Fatal(err)
	}

	out := b.String()
	if len(out) == 0 {
		t.Fatalf("unexpected result length: %d == 0", len(out))
	}

	if match, _ := regexp.Match(`^mytool,host=[^,]+,baaz\\ gogo=gu\\,gu,foo=bar testval=1i,testvalue=2i,testvalue2=-3i,testvalue3=\"foobar\\\"baz\",testvaluebool=false,testvalueflt32=3.1415927,testvalueflt64=1.29e-24,testvaluetime=`, []byte(out)); !match {
		t.Fatalf("unexpected match content: %s", out)
	}
}

func TestEncoderTypeFail(t *testing.T) {
	var b bytes.Buffer

	ile := NewEncoder(&b)
	tags := make(map[string]string)
	err := ile.Encode("mytool", testStructInvalidType, tags)
	if err == nil {
		t.Fatal(err)
	}
}

func TestEncoderStringTooLongFail(t *testing.T) {
	var b bytes.Buffer

	ile := NewEncoder(&b)
	tags := make(map[string]string)
	err := ile.Encode("mytool", testStructStringLong, tags)
	if err == nil {
		t.Fatal(err)
	}
}

func TestEncoderPartUntagged(t *testing.T) {
	var b bytes.Buffer

	ile := NewEncoder(&b)
	tags := make(map[string]string)
	err := ile.Encode("mytool", testStructPartUntagged, tags)
	if err != nil {
		t.Fatal(err)
	}

	out := b.String()
	if match, _ := regexp.Match(`^mytool,host=[^,]+ testval=1i,testvalue=2i`, []byte(out)); !match {
		t.Fatalf("unexpected match content: %s", out)
	}
}

func TestEncoderAllUntagged(t *testing.T) {
	var b bytes.Buffer

	ile := NewEncoder(&b)
	tags := make(map[string]string)
	err := ile.Encode("mytool", testStructAllUntagged, tags)
	if err != nil {
		t.Fatal(err)
	}

	out := b.String()
	if len(out) != 0 {
		t.Fatalf("unexpected result length: %d != 0", len(out))
	}
}
