package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// stagePendingBootstrapConversations copies committed phone history only for selected chats.
func (wa *WhatsAppClient) stagePendingBootstrapConversations(ctx context.Context, blob *waHistorySync.HistorySync) {
	for _, conv := range blob.GetConversations() {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			wa.UserLogin.Log.Err(err).Str("chat_jid", conv.GetID()).Msg("Cannot stage WhatsApp history for invalid chat")
			continue
		}
		if jid.Server == types.DefaultUserServer {
			jid, err = wa.GetStore().LIDs.GetLIDForPN(ctx, jid)
			if err != nil || jid.IsEmpty() {
				wa.UserLogin.Log.Err(err).Str("chat_jid", conv.GetID()).Msg("Cannot stage WhatsApp history without chat LID")
				continue
			}
		}
		key := wa.makeWAPortalKey(jid)
		job, err := wa.Main.Bridge.DB.GetBootstrapJob(ctx, wa.UserLogin.ID, key)
		if err != nil {
			wa.UserLogin.Log.Err(err).Object("portal_key", key).Msg("Cannot inspect pending WhatsApp bootstrap")
			continue
		}
		if job == nil {
			continue
		}
		if job.Status == "ready" {
			if err = wa.stageLateBootstrapHistory(ctx, jid, key, conv); err != nil {
				wa.UserLogin.Log.Err(err).Object("portal_key", key).Msg("Cannot inspect late WhatsApp history")
				if statusErr := wa.Main.Bridge.DB.SetBootstrapStatus(ctx, wa.UserLogin.ID, key, "reconcile", err.Error()); statusErr != nil {
					wa.UserLogin.Log.Err(statusErr).Object("portal_key", key).Msg("Cannot record late history inspection failure")
				}
			}
			continue
		}
		if job.Status == "reconcile" {
			continue
		}
		if blob.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND && strings.HasPrefix(job.Cursor, "phone:") {
			var pending phoneBootstrapCursor
			err = json.Unmarshal([]byte(strings.TrimPrefix(job.Cursor, "phone:")), &pending)
			if err == nil {
				var older []*waWeb.WebMessageInfo
				older, _, _, err = wa.Main.DB.Message.BootstrapPage(ctx, wa.UserLogin.ID, jid, pending.Keyset, 1)
				if err == nil && len(older) == 0 {
					meta, metaErr := wa.Main.DB.Conversation.Get(ctx, wa.UserLogin.ID, jid)
					if metaErr != nil {
						err = metaErr
					} else if meta != nil && meta.EndOfHistoryTransferType != nil &&
						*meta.EndOfHistoryTransferType == waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS {
						err = fmt.Errorf("WhatsApp phone denies access to older messages in %s", jid)
					} else if meta == nil || !phoneHistoryFinished(meta.EndOfHistoryTransferType) {
						err = fmt.Errorf("WhatsApp phone history response made no progress for %s", jid)
					}
				}
			}
		}
		var portal *bridgev2.Portal
		if err == nil {
			portal, err = wa.Main.Bridge.GetPortalByKey(ctx, key)
		}
		if err == nil {
			err = wa.StageBootstrapHistory(ctx, portal)
		}
		if err != nil {
			wa.UserLogin.Log.Err(err).Object("portal_key", key).Msg("Cannot stage selected WhatsApp history")
			if statusErr := wa.Main.Bridge.DB.SetBootstrapStatus(ctx, wa.UserLogin.ID, key, "incomplete", err.Error()); statusErr != nil {
				wa.UserLogin.Log.Err(statusErr).Object("portal_key", key).Msg("Cannot record incomplete WhatsApp bootstrap")
			}
		} else {
			job, err = wa.Main.Bridge.DB.GetBootstrapJob(ctx, wa.UserLogin.ID, key)
			if err != nil {
				wa.UserLogin.Log.Err(err).Object("portal_key", key).Msg("Cannot inspect staged WhatsApp completion")
			} else if job != nil && job.SourceComplete {
				wa.UserLogin.ResumeBootstrapJobs()
			}
		}
	}
}

// stageLateBootstrapHistory never inserts older events into an occupied room.
func (wa *WhatsAppClient) stageLateBootstrapHistory(ctx context.Context, jid types.JID, key networkid.PortalKey, conv *waHistorySync.Conversation) error {
	var unseen []database.BootstrapItem
	for _, raw := range conv.GetMessages() {
		if raw.GetMessage() == nil {
			return fmt.Errorf("late WhatsApp history contains an empty message")
		}
		item, err := wa.bootstrapHistoryItem(ctx, jid, raw.GetMessage())
		if err != nil {
			return err
		}
		id := networkid.MessageID(strings.TrimPrefix(item.StableID, "wa-history:"))
		mapped, err := wa.Main.Bridge.DB.Message.GetAllPartsByID(ctx, key.Receiver, id)
		if err != nil {
			return err
		}
		if len(mapped) == 0 {
			unseen = append(unseen, item)
		}
	}
	if len(unseen) == 0 {
		return nil
	}
	return wa.Main.Bridge.DB.StageBootstrapPage(ctx, wa.UserLogin.ID, key, unseen, "", false)
}

// phoneHistoryFinished requires WhatsApp to report no more messages on the phone.
func phoneHistoryFinished(end *waHistorySync.Conversation_EndOfHistoryTransferType) bool {
	return end != nil && *end == waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY
}

type phoneBootstrapCursor struct {
	Keyset string `json:"keyset"`
	Anchor string `json:"anchor"`
}

func (wa *WhatsAppClient) requestBootstrapPhoneHistory(ctx context.Context, jid types.JID, encoded string) error {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	var msg waWeb.WebMessageInfo
	if err = proto.Unmarshal(raw, &msg); err != nil {
		return err
	}
	anchor, err := wa.Client.ParseWebMessage(jid, &msg)
	if err != nil {
		return fmt.Errorf("parse WhatsApp history request anchor: %w", err)
	}
	return wa.RequestHistoryFromPhone(ctx, &anchor.Info)
}

func (wa *WhatsAppClient) bootstrapHistoryItem(ctx context.Context, jid types.JID, msg *waWeb.WebMessageInfo) (database.BootstrapItem, error) {
	evt, err := wa.Client.ParseWebMessage(jid, msg)
	if err != nil {
		return database.BootstrapItem{}, fmt.Errorf("parse WhatsApp cached history: %w", err)
	}
	if evt.Info.ID == "" || !wa.ensureAltJIDs(ctx, &evt.Info.MessageSource, false) {
		return database.BootstrapItem{}, fmt.Errorf("resolve WhatsApp cached history sender or message ID")
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return database.BootstrapItem{}, fmt.Errorf("encode WhatsApp cached history: %w", err)
	}
	msgID := waid.MakeMessageIDWithAltSender(evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt, evt.Info.ID)
	return database.BootstrapItem{
		StableID: "wa-history:" + string(msgID),
		Kind:     "history",
		Version:  1,
		Payload:  base64.StdEncoding.EncodeToString(raw),
		SourceTS: evt.Info.Timestamp.UnixMilli(),
	}, nil
}

// StageBootstrapHistory copies bounded cached and accessible phone pages before room creation.
func (wa *WhatsAppClient) StageBootstrapHistory(ctx context.Context, portal *bridgev2.Portal) error {
	jid, err := waid.ParsePortalID(portal.ID)
	if err != nil {
		return err
	}
	if jid.Server == types.DefaultUserServer {
		jid, err = wa.GetStore().LIDs.GetLIDForPN(ctx, jid)
		if err != nil {
			return fmt.Errorf("resolve WhatsApp phone-number chat: %w", err)
		}
		if jid.IsEmpty() {
			return fmt.Errorf("WhatsApp phone-number chat has no LID mapping")
		}
	}
	job, err := wa.Main.Bridge.DB.GetBootstrapJob(ctx, wa.UserLogin.ID, portal.PortalKey)
	if err != nil {
		return err
	}
	if job != nil && job.SourceComplete {
		return nil
	}
	cursor := ""
	if job != nil {
		cursor = job.Cursor
	}
	if strings.HasPrefix(cursor, "phone:") {
		var pending phoneBootstrapCursor
		if err = json.Unmarshal([]byte(strings.TrimPrefix(cursor, "phone:")), &pending); err != nil || pending.Keyset == "" || pending.Anchor == "" {
			return fmt.Errorf("invalid WhatsApp phone history cursor: %v", err)
		}
		older, _, _, err := wa.Main.DB.Message.BootstrapPage(ctx, wa.UserLogin.ID, jid, pending.Keyset, 1)
		if err != nil {
			return err
		}
		if len(older) == 0 {
			conv, err := wa.Main.DB.Conversation.Get(ctx, wa.UserLogin.ID, jid)
			if err != nil {
				return err
			}
			if conv != nil && conv.EndOfHistoryTransferType != nil &&
				*conv.EndOfHistoryTransferType == waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS {
				return fmt.Errorf("WhatsApp phone denies access to older messages in %s", jid)
			}
			if conv == nil || !phoneHistoryFinished(conv.EndOfHistoryTransferType) {
				return wa.requestBootstrapPhoneHistory(ctx, jid, pending.Anchor)
			}
			cursor = ""
		} else {
			cursor = pending.Keyset
		}
	}
	for {
		messages, next, more, err := wa.Main.DB.Message.BootstrapPage(ctx, wa.UserLogin.ID, jid, cursor, 50)
		if err != nil {
			return fmt.Errorf("load WhatsApp cached history: %w", err)
		}
		items := make([]database.BootstrapItem, 0, len(messages))
		for _, msg := range messages {
			item, err := wa.bootstrapHistoryItem(ctx, jid, msg)
			if err != nil {
				return err
			}
			items = append(items, item)
		}
		complete := false
		var phoneAnchor string
		var phoneDenied bool
		var phoneAnchorMissing bool
		if !more {
			conv, err := wa.Main.DB.Conversation.Get(ctx, wa.UserLogin.ID, jid)
			if err != nil {
				return fmt.Errorf("load WhatsApp history completion marker: %w", err)
			}
			complete = conv != nil && phoneHistoryFinished(conv.EndOfHistoryTransferType)
			phoneDenied = conv != nil && conv.EndOfHistoryTransferType != nil &&
				*conv.EndOfHistoryTransferType == waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS
			if complete {
				next = ""
			} else if conv != nil && historyAvailableOnPhone(conv.EndOfHistoryTransferType) && len(items) != 0 {
				phoneAnchor = items[len(items)-1].Payload
				encoded, err := json.Marshal(phoneBootstrapCursor{Keyset: next, Anchor: phoneAnchor})
				if err != nil {
					return err
				}
				next = "phone:" + string(encoded)
			} else {
				phoneAnchorMissing = conv != nil && historyAvailableOnPhone(conv.EndOfHistoryTransferType)
				next = ""
			}
		}
		if err = wa.Main.Bridge.DB.StageBootstrapPage(ctx, wa.UserLogin.ID, portal.PortalKey, items, next, complete); err != nil {
			return fmt.Errorf("persist WhatsApp cached history page: %w", err)
		}
		if !more {
			if phoneDenied {
				return fmt.Errorf("WhatsApp phone denies access to older messages in %s", jid)
			}
			if phoneAnchorMissing {
				return fmt.Errorf("WhatsApp phone has more history but no cached request anchor in %s", jid)
			}
			if phoneAnchor != "" {
				return wa.requestBootstrapPhoneHistory(ctx, jid, phoneAnchor)
			}
			return nil
		}
		cursor = next
	}
}

func (wa *WhatsAppClient) ConvertBootstrapHistory(ctx context.Context, portal *bridgev2.Portal, item database.BootstrapItem) (*bridgev2.BackfillMessage, error) {
	if item.Kind != "history" || item.Version != 1 {
		return nil, fmt.Errorf("unsupported WhatsApp history item %s version %d", item.Kind, item.Version)
	}
	raw, err := base64.StdEncoding.DecodeString(item.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode WhatsApp history item: %w", err)
	}
	var msg waWeb.WebMessageInfo
	if err = proto.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("parse WhatsApp history item: %w", err)
	}
	jid, err := waid.ParsePortalID(portal.ID)
	if err != nil {
		return nil, err
	}
	if jid.Server == types.DefaultUserServer {
		jid, err = wa.GetStore().LIDs.GetLIDForPN(ctx, jid)
		if err != nil || jid.IsEmpty() {
			return nil, fmt.Errorf("resolve WhatsApp history chat LID: %w", err)
		}
	}
	evt, err := wa.Client.ParseWebMessage(jid, &msg)
	if err != nil {
		return nil, fmt.Errorf("parse WhatsApp history item: %w", err)
	}
	if !wa.ensureAltJIDs(ctx, &evt.Info.MessageSource, false) {
		return nil, fmt.Errorf("resolve WhatsApp history sender")
	}
	msgID := waid.MakeMessageIDWithAltSender(evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt, evt.Info.ID)
	if item.StableID != "wa-history:"+string(msgID) {
		return nil, fmt.Errorf("WhatsApp history item identity changed")
	}
	isViewOnce := evt.IsViewOnce || evt.IsViewOnceV2 || evt.IsViewOnceV2Extension
	converted, mediaReq := wa.convertHistorySyncMessage(ctx, portal, &evt.Info, evt.Message, evt.RawMessage, isViewOnce, msg.Reactions)
	if mediaReq != nil {
		converted.AfterBootstrapImport = func(ctx context.Context) error {
			inserted, err := wa.Main.DB.MediaRequest.PutBootstrapRequest(ctx, mediaReq)
			if err != nil {
				return fmt.Errorf("persist WhatsApp media request: %w", err)
			}
			if inserted && wa.Main.Config.HistorySync.MediaRequests.AutoRequestMedia &&
				wa.Main.Config.HistorySync.MediaRequests.RequestMethod == MediaRequestMethodImmediate {
				go wa.sendMediaRequest(context.WithoutCancel(ctx), mediaReq)
			}
			return nil
		}
	}
	return converted, nil
}

var _ bridgev2.PortalBootstrapSource = (*WhatsAppClient)(nil)
