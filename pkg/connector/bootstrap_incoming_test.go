package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestStagedWhatsAppMessageRoundTrip(t *testing.T) {
	chat := types.NewJID("123", types.DefaultUserServer)
	msg := &waE2E.Message{Conversation: proto.String("private message")}
	original := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "remote-id", Timestamp: time.Unix(42, 0),
		},
		Message: msg, RawMessage: msg, IsViewOnce: true,
	}
	wrapped := &WAMessageEvent{
		MessageInfoWrapper: &MessageInfoWrapper{Info: original.Info},
		Message:            msg, MsgEvent: original, parsedMessageType: "text",
	}
	payload, err := encodeStagedWAMessage(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	decoded, messageType, _, err := decodeStagedWAMessage(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Info.Chat != chat || decoded.Info.Sender != chat || decoded.Info.ID != original.Info.ID ||
		!decoded.Info.Timestamp.Equal(original.Info.Timestamp) || !decoded.IsViewOnce || messageType != "text" ||
		!proto.Equal(decoded.Message, msg) || !proto.Equal(decoded.RawMessage, msg) {
		t.Fatalf("WhatsApp event changed across restart: %+v", decoded.Info)
	}
}

func TestUnselectedIncomingWhatsAppMessageUsesLiveDelivery(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "live.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db := database.New("wa", database.MetaTypes{}, raw)
	if err = db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	bridge := &bridgev2.Bridge{DB: db, Config: &bridgeconfig.BridgeConfig{}}
	wa := &WhatsAppClient{Main: &WhatsAppConnector{Bridge: bridge}, UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}}}
	chat := types.NewJID("123", types.GroupServer)
	evt := &WAMessageEvent{MessageInfoWrapper: &MessageInfoWrapper{wa: wa, Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "new-message", Timestamp: time.Unix(42, 0),
	}}}
	staged, err := wa.stageBootstrapWAMessage(ctx, evt)
	if err != nil || staged {
		t.Fatalf("new message did not take live delivery path: staged=%t err=%v", staged, err)
	}
	job, err := db.GetBootstrapJob(ctx, wa.UserLogin.ID, evt.GetPortalKey())
	if err != nil || job != nil {
		t.Fatalf("new message unexpectedly selected history bootstrap: %+v %v", job, err)
	}
}

func TestReceiptStagedBeforeFirstPortalRecord(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "incoming.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db := database.New("wa", database.MetaTypes{}, raw)
	if err = db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	bridge := &bridgev2.Bridge{DB: db, Config: &bridgeconfig.BridgeConfig{}}
	wa := &WhatsAppClient{Main: &WhatsAppConnector{Bridge: bridge}, UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}}}
	chat := types.NewJID("123", types.GroupServer)
	key := wa.makeWAPortalKey(chat)
	if err = db.EnsureBootstrapJob(ctx, wa.UserLogin.ID, key); err != nil {
		t.Fatal(err)
	}
	receipt := &events.Receipt{MessageSource: types.MessageSource{Chat: chat, Sender: types.NewJID("456", types.DefaultUserServer)}, MessageIDs: []types.MessageID{"incoming"}, Type: types.ReceiptTypeRead}
	staged, err := wa.stageBootstrapWAReceipt(ctx, receipt)
	if err != nil || !staged {
		t.Fatalf("selected chat lost receipt before portal record: staged=%v err=%v", staged, err)
	}
	items, err := db.GetPendingBootstrapItems(ctx, wa.UserLogin.ID, key, 10)
	if err != nil || len(items) != 1 || items[0].Kind != "incoming-receipt" {
		t.Fatalf("incoming receipt was not durable: %+v %v", items, err)
	}
}

func TestStagedWhatsAppReceiptRoundTrip(t *testing.T) {
	chat := types.NewJID("123", types.DefaultUserServer)
	receipt := &events.Receipt{
		MessageSource: types.MessageSource{Chat: chat, Sender: chat},
		MessageIDs:    []types.MessageID{"one", "two"},
		Timestamp:     time.Unix(42, 0), Type: types.ReceiptTypeRead,
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	item := database.BootstrapItem{
		StableID: fmt.Sprintf("wa-receipt:%x", sha256.Sum256(payload)),
		Kind:     "incoming-receipt", Version: 1, Payload: string(payload),
	}
	decoded, err := DecodeStagedWhatsAppReceipt(item)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Chat != chat || len(decoded.MessageIDs) != 2 || decoded.MessageIDs[1] != "two" ||
		decoded.Type != types.ReceiptTypeRead || !decoded.Timestamp.Equal(receipt.Timestamp) {
		t.Fatalf("WhatsApp receipt changed across restart: %+v", decoded)
	}
	item.Payload = `{"invalid":"receipt"}`
	if _, err = DecodeStagedWhatsAppReceipt(item); err == nil {
		t.Fatal("changed receipt accepted for saved identity")
	}
}
