# fluxline

Encoder for Golang to prepare sets of metrics in [InfluxDB's Line Protocol](https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_reference) format. As input, we use structs annotated with the `influx` tag, similar to how `encoding/json` works.

Supports the following Go builtin types as fields:

 - `string`
 - `int32`, `int64`, `int16`, `int8`, `int`, `uint32`, `uint64`, `uint16`, `uint8`, `uint`
 - `float64`, `float32`
 - `bool`
 - `time.Time`

## Remarks

Not thread safe. If the struct is modified elsewhere concurrently, one would need to protect the read access required for encoding.

## Get the code

```bash
go get github.com/DCSO/fluxline
```

## Usage example

```golang
package main

import (
  "bytes"
  "fmt"
  "log"

  "github.com/DCSO/fluxline"
)

const measurement = "example"

type MyCounts struct {
  Success uint64 `influx:"success"`
  Error   uint64 `influx:"error"`
}

func main() {
  var counts MyCounts
  var b bytes.Buffer

  // ...
  counts.Success++
  // ...
  counts.Success++
  // ...
  counts.Error++
  // ...

  tags := make(map[string]string)
  tags["foo"] = "bar"

  encoder := fluxline.NewEncoder(&b)
  err := encoder.Encode(measurement, counts, tags)
  if err != nil {
		log.Fatal(err)
	}
  fmt.Print(b.String())
}
```
