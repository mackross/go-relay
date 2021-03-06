package relay

import (
	"testing"
)

func TestQueueName(t *testing.T) {
	if queueName("test") != "relay.test" {
		t.Fatalf("bad queue name")
	}
}

func TestChannelName(t *testing.T) {
	names := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		name, err := channelName()
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if name == "" {
			t.Fatalf("expected name")
		}
		if _, ok := names[name]; ok {
			t.Fatalf("expected unique name!")
		}
		names[name] = struct{}{}
	}
}
