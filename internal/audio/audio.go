// Package audio holds PCM constants and millisecond ↔ byte conversions for the
// single audio format Cascade supports in v1: 16-bit little-endian mono PCM at
// 24 kHz.
//
// Layering: audio is a leaf package and imports nothing under internal/.
package audio

import "encoding/binary"

const (
	// SampleRate is the only sample rate supported in v1.
	SampleRate = 24000
	// BytesPerSample is 16-bit PCM.
	BytesPerSample = 2
	// BytesPerMs is the number of PCM bytes in one millisecond of audio.
	BytesPerMs = SampleRate * BytesPerSample / 1000
)

// MsToBytes converts a duration in milliseconds to a PCM byte count.
func MsToBytes(ms int) int { return ms * BytesPerMs }

// BytesToMs converts a PCM byte count to whole milliseconds (rounded down).
func BytesToMs(n int) int { return n / BytesPerMs }

// AlignDown rounds a byte offset down to a whole sample boundary.
func AlignDown(n int) int { return n - n%BytesPerSample }

// Sample returns the i-th 16-bit sample of pcm.
func Sample(pcm []byte, i int) int16 {
	return int16(binary.LittleEndian.Uint16(pcm[i*BytesPerSample:]))
}

// PutSample writes v as the i-th 16-bit sample of pcm.
func PutSample(pcm []byte, i int, v int16) {
	binary.LittleEndian.PutUint16(pcm[i*BytesPerSample:], uint16(v))
}
