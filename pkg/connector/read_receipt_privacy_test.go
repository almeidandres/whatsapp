package connector

import (
	"context"
	"testing"
)

func TestMatrixReadStateStaysLocal(t *testing.T) {
	client := &WhatsAppClient{}
	if err := client.HandleMatrixReadReceipt(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := client.HandleMarkedUnread(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
