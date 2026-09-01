package audio

import "testing"

func TestConversions(t *testing.T) {
	if BytesPerMs != 48 {
		t.Fatalf("BytesPerMs = %d", BytesPerMs)
	}
	if MsToBytes(1000) != 48000 || BytesToMs(48000) != 1000 || BytesToMs(47) != 0 {
		t.Fatal("ms/byte conversion wrong")
	}
	if AlignDown(7) != 6 || AlignDown(8) != 8 {
		t.Fatal("AlignDown wrong")
	}
	buf := make([]byte, 4)
	PutSample(buf, 1, -2)
	if Sample(buf, 1) != -2 || Sample(buf, 0) != 0 {
		t.Fatal("sample round trip wrong")
	}
}
