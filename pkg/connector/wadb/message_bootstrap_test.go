package wadb

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"time"
)

func TestBootstrapMediaRequestWaitsForMessageMapping(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "media.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	core := database.New("wa", database.MetaTypes{}, raw)
	if err = core.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	wa := New("wa", raw, zerolog.Nop())
	if err = wa.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(ctx, `INSERT INTO "user"(bridge_id,mxid) VALUES ($1,$2)`, "wa", "@owner:localhost"); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(ctx, `INSERT INTO user_login(bridge_id,user_mxid,id,remote_name,metadata) VALUES ($1,$2,$3,$4,$5)`, "wa", "@owner:localhost", "login", "phone", "{}"); err != nil {
		t.Fatal(err)
	}
	key := networkid.PortalKey{ID: "thread", Receiver: "login"}
	if err = core.Portal.Insert(ctx, &database.Portal{PortalKey: key}); err != nil {
		t.Fatal(err)
	}
	req := &MediaRequest{UserLoginID: "login", MessageID: "media", PortalKey: key, MediaKey: []byte("key"), Status: MediaBackfillRequestStatusNotRequested}
	if _, err = wa.MediaRequest.PutBootstrapRequest(ctx, req); err == nil {
		t.Fatal("media request saved before Matrix message mapping")
	}
	if err = core.Ghost.Insert(ctx, &database.Ghost{ID: "sender"}); err != nil {
		t.Fatal(err)
	}
	if err = core.Message.Insert(ctx, &database.Message{Room: key, ID: "media", MXID: "$event", SenderID: "sender", SenderMXID: id.UserID("@sender:localhost"), Timestamp: time.UnixMilli(2)}); err != nil {
		t.Fatal(err)
	}
	inserted, err := wa.MediaRequest.PutBootstrapRequest(ctx, req)
	if err != nil || !inserted {
		t.Fatalf("media request not saved after mapping: %t %v", inserted, err)
	}
	if err = wa.MediaRequest.Put(ctx, &MediaRequest{UserLoginID: "login", MessageID: "media", PortalKey: key, Status: MediaBackfillRequestStatusRequested}); err != nil {
		t.Fatal(err)
	}
	inserted, err = wa.MediaRequest.PutBootstrapRequest(ctx, req)
	if err != nil || inserted {
		t.Fatalf("retry reset existing media request: %t %v", inserted, err)
	}
	requests, err := wa.MediaRequest.GetUnrequestedForUserLogin(ctx, "login")
	if err != nil || len(requests) != 0 {
		t.Fatalf("retry reset a requested media item: %+v %v", requests, err)
	}
}

func TestBootstrapPageKeepsSameTimestampMessages(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "cache.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_, err = raw.Exec(ctx, `CREATE TABLE whatsapp_history_sync_message (
		bridge_id TEXT, user_login_id TEXT, chat_jid TEXT, sender_jid TEXT,
		message_id TEXT, timestamp BIGINT, data BLOB, inserted_time BIGINT)`)
	if err != nil {
		t.Fatal(err)
	}
	chat := types.NewJID("123", types.DefaultUserServer)
	payload, err := proto.Marshal(&waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{ID: proto.String("raw"), RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(false)},
		MessageTimestamp: proto.Uint64(10),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id string
		ts int64
	}{{"a", 10}, {"b", 10}, {"c", 10}, {"d", 10}, {"older", 9}} {
		if _, err = raw.Exec(ctx, `INSERT INTO whatsapp_history_sync_message VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, "wa", "login", chat, "sender", row.id, row.ts, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	query := &MessageQuery{BridgeID: "wa", Database: raw}
	var found []string
	var cursor string
	for {
		messages, next, more, err := query.BootstrapPage(ctx, "login", chat, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) >= 3 || len(messages) != []int{2, 2, 1}[len(found)] {
			t.Fatalf("unexpected page size %d at page %d", len(messages), len(found))
		}
		var marker struct {
			MessageID string `json:"i"`
		}
		if err = json.Unmarshal([]byte(next), &marker); err != nil {
			t.Fatal(err)
		}
		// The cursor identifies the last item in each page; checking page sizes also detects skipped ties.
		found = append(found, marker.MessageID)
		if !more {
			break
		}
		cursor = next
	}
	if !reflect.DeepEqual(found, []string{"c", "a", "older"}) {
		t.Fatalf("same-timestamp page boundaries lost messages: %v", found)
	}
}
