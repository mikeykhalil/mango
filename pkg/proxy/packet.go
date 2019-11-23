package proxy

import (
	"bytes"
	"fmt"
	"strconv"
)

var (
	fieldSeparator    = []byte("|")
	tagSeparator      = []byte(",")
	valueSeparator    = []byte(":")
	metricFieldPrefix = []byte("#")
	hostTagPrefix     = []byte("host:")
)

const (
	// TODO: change this value
	MaxUDPPacketBytes = 1024
)

type DogStatsdPacket struct {
	MetricName  []byte
	MetricValue []byte
	SampleRate  float64
	Type        []byte
	Tags        []string
	raw         []byte
}

// dogstatsd message format: <METRIC_NAME>:<VALUE>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
func (dsp *DogStatsdPacket) Parse(buf []byte) error {
	dsp.raw = buf
	separatorCount := bytes.Count(buf, fieldSeparator)
	if separatorCount < 1 || separatorCount > 3 {
		return fmt.Errorf("invalid field number for %q", buf)
	}

	nameAndVal, remainder := nextField(buf, fieldSeparator)
	name, val := nextField(nameAndVal, valueSeparator)
	dsp.MetricName, dsp.MetricValue = name, val

	t, remainder := nextField(remainder, fieldSeparator)

	dsp.Type = t
	dsp.raw = buf
	dsp.SampleRate = 1.0

	var rawMetadataField []byte
	for remainder != nil {
		rawMetadataField, remainder = nextField(remainder, fieldSeparator)
		if bytes.HasPrefix(rawMetadataField, []byte("#")) {
			dsp.Tags = parseTags(rawMetadataField[1:])
		} else if bytes.HasPrefix(rawMetadataField, []byte("@")) {
			rawSampleRate := rawMetadataField[1:]
			var err error
			sampleRate, err := strconv.ParseFloat(string(rawSampleRate), 64)
			if err != nil {
				return fmt.Errorf("invalid sample value for %q", buf)
			}
			dsp.SampleRate = sampleRate
		}
	}
	return nil
}

func parseTags(tags []byte) []string {
	tagsList := make([]string, 0, bytes.Count(tags, tagSeparator)+1)
	remainder := tags
	var tag []byte
	for remainder != nil {
		tag, remainder = nextField(remainder, tagSeparator)
		tagsList = append(tagsList, string(tag))
	}
	return tagsList
}

// nextField returns the data found before the given separator and
// the remainder, as a no-heap alternative to bytes.Split.
// If the separator is not found, the remainder is nil.
func nextField(slice, sep []byte) ([]byte, []byte) {
	sepIndex := bytes.Index(slice, sep)
	if sepIndex == -1 {
		return slice, nil
	}
	return slice[:sepIndex], slice[sepIndex+1:]
}

// TODO: Conditional appending of tags, more efficient serialization...
func (dsp *DogStatsdPacket) Serialize() string {
	return fmt.Sprintf("%v:%v|%v|@%v|%v", string(dsp.MetricName), string(dsp.MetricValue), string(dsp.Type), dsp.SampleRate, dsp.Tags)
}
