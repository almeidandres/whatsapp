package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

type stagedWAMessage struct {
	Event            events.Message `json:"event"`
	Message          []byte         `json:"message"`
	RawMessage       []byte         `json:"raw_message,omitempty"`
	SourceWebMessage []byte         `json:"source_web_message,omitempty"`
	MessageType      string         `json:"message_type"`
	DontRenderEdited bool           `json:"dont_render_edited"`
}

func encodeStagedWAMessage(evt *WAMessageEvent) (string, error) {
	if evt.MsgEvent == nil || evt.Message == nil || evt.Info.ID == "" {
		return "", fmt.Errorf("WhatsApp event has no replayable message or ID")
	}
	stored := stagedWAMessage{
		Event:            *evt.MsgEvent,
		MessageType:      evt.parsedMessageType,
		DontRenderEdited: evt.dontRenderEdited,
	}
	stored.Event.Message = nil
	stored.Event.RawMessage = nil
	stored.Event.SourceWebMsg = nil
	var err error
	if stored.Message, err = proto.Marshal(evt.Message); err != nil {
		return "", err
	}
	if evt.MsgEvent.RawMessage != nil {
		if stored.RawMessage, err = proto.Marshal(evt.MsgEvent.RawMessage); err != nil {
			return "", err
		}
	}
	if evt.MsgEvent.SourceWebMsg != nil {
		if stored.SourceWebMessage, err = proto.Marshal(evt.MsgEvent.SourceWebMsg); err != nil {
			return "", err
		}
	}
	data, err := json.Marshal(stored)
	return string(data), err
}

func decodeStagedWAMessage(payload string) (*events.Message, string, bool, error) {
	var stored stagedWAMessage
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		return nil, "", false, err
	}
	if stored.Event.Info.ID == "" || len(stored.Message) == 0 {
		return nil, "", false, fmt.Errorf("WhatsApp staged message has no ID or content")
	}
	stored.Event.Message = &waE2E.Message{}
	if err := proto.Unmarshal(stored.Message, stored.Event.Message); err != nil {
		return nil, "", false, err
	}
	if len(stored.RawMessage) != 0 {
		stored.Event.RawMessage = &waE2E.Message{}
		if err := proto.Unmarshal(stored.RawMessage, stored.Event.RawMessage); err != nil {
			return nil, "", false, err
		}
	}
	if len(stored.SourceWebMessage) != 0 {
		stored.Event.SourceWebMsg = &waWeb.WebMessageInfo{}
		if err := proto.Unmarshal(stored.SourceWebMessage, stored.Event.SourceWebMsg); err != nil {
			return nil, "", false, err
		}
	}
	return &stored.Event, stored.MessageType, stored.DontRenderEdited, nil
}

// stageBootstrapWAMessage persists an incoming event before its handler acknowledges it.
func (wa *WhatsAppClient) stageBootstrapWAMessage(ctx context.Context, evt *WAMessageEvent) (bool, error) {
	key := evt.GetPortalKey()
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		return false, err
	}
	resume := portal == nil
	if portal != nil {
		job, err := wa.Main.Bridge.DB.GetBootstrapJob(ctx, wa.UserLogin.ID, key)
		if err != nil {
			return false, err
		}
		if portal.MXID != "" && (job == nil || job.Status == "ready") {
			return false, nil
		}
		resume = job == nil || portal.MXID != ""
	}
	payload, err := encodeStagedWAMessage(evt)
	if err != nil {
		return false, err
	}
	item := database.BootstrapItem{
		StableID: "wa-incoming:" + string(evt.GetID()),
		Kind:     "incoming",
		Version:  1,
		Payload:  payload,
		SourceTS: evt.Info.Timestamp.UnixMilli(),
	}
	if err = wa.Main.Bridge.DB.StageBootstrapIncoming(ctx, wa.UserLogin.ID, key, item); err != nil {
		return false, fmt.Errorf("stage WhatsApp message before acknowledgement: %w", err)
	}
	if portal == nil {
		if _, err = wa.Main.Bridge.GetPortalByKey(ctx, key); err != nil {
			return false, fmt.Errorf("create selected WhatsApp portal record: %w", err)
		}
	}
	if resume {
		wa.UserLogin.ResumeBootstrapJobs()
	}
	return true, nil
}

// stageBootstrapWAReceipt retains incoming read and delivery receipts while a selected room is pending.
func (wa *WhatsAppClient) stageBootstrapWAReceipt(ctx context.Context, evt *events.Receipt) (bool, error) {
	chat := evt.Chat
	if chat.Server == types.DefaultUserServer {
		lid, err := wa.GetStore().LIDs.GetLIDForPN(ctx, chat)
		if err != nil {
			return false, err
		}
		if !lid.IsEmpty() {
			chat = lid
		}
	}
	key := wa.makeWAPortalKey(chat)
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		return false, err
	}
	if portal != nil {
		key = portal.PortalKey
	}
	job, err := wa.Main.Bridge.DB.GetBootstrapJob(ctx, wa.UserLogin.ID, key)
	if err != nil || job == nil || job.Status == "ready" {
		return false, err
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return false, err
	}
	item := database.BootstrapItem{
		StableID: fmt.Sprintf("wa-receipt:%x", sha256.Sum256(payload)),
		Kind:     "incoming-receipt",
		Version:  1,
		Payload:  string(payload),
		SourceTS: evt.Timestamp.UnixMilli(),
	}
	if err = wa.Main.Bridge.DB.StageBootstrapIncoming(ctx, wa.UserLogin.ID, key, item); err != nil {
		return false, fmt.Errorf("stage WhatsApp receipt before acknowledgement: %w", err)
	}
	if portal != nil && portal.MXID != "" {
		wa.UserLogin.ResumeBootstrapJobs()
	}
	return true, nil
}

func DecodeStagedWhatsAppReceipt(item database.BootstrapItem) (*events.Receipt, error) {
	if item.Kind != "incoming-receipt" || item.Version != 1 {
		return nil, fmt.Errorf("unsupported WhatsApp receipt item %s version %d", item.Kind, item.Version)
	}
	if item.StableID != fmt.Sprintf("wa-receipt:%x", sha256.Sum256([]byte(item.Payload))) {
		return nil, fmt.Errorf("WhatsApp receipt identity changed")
	}
	var receipt events.Receipt
	if err := json.Unmarshal([]byte(item.Payload), &receipt); err != nil {
		return nil, fmt.Errorf("decode WhatsApp receipt: %w", err)
	}
	return &receipt, nil
}

func (wa *WhatsAppClient) ReplayBootstrapIncoming(ctx context.Context, portal *bridgev2.Portal, item database.BootstrapItem) error {
	switch item.Kind {
	case "incoming":
		evt, err := wa.DecodeStagedWhatsAppEvent(item)
		if err != nil {
			return err
		}
		return portal.HandleBootstrapEvent(ctx, wa.UserLogin, evt)
	case "incoming-receipt":
		receipt, err := DecodeStagedWhatsAppReceipt(item)
		if err != nil {
			return err
		}
		if !wa.ensureAltJIDs(ctx, &receipt.MessageSource, true) {
			return fmt.Errorf("resolve staged WhatsApp receipt sender")
		}
		var evtType bridgev2.RemoteEventType
		switch receipt.Type {
		case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
			evtType = bridgev2.RemoteEventReadReceipt
		case types.ReceiptTypeDelivered:
			evtType = bridgev2.RemoteEventDeliveryReceipt
		default:
			return fmt.Errorf("unsupported staged WhatsApp receipt type %s", receipt.Type)
		}
		return portal.HandleBootstrapEvent(ctx, wa.UserLogin, wa.makeWAReceiptEvent(ctx, receipt, evtType))
	default:
		return fmt.Errorf("unsupported WhatsApp incoming item kind %s", item.Kind)
	}
}

func (wa *WhatsAppClient) DecodeStagedWhatsAppEvent(item database.BootstrapItem) (*WAMessageEvent, error) {
	if item.Kind != "incoming" || item.Version != 1 {
		return nil, fmt.Errorf("unsupported WhatsApp event item %s version %d", item.Kind, item.Version)
	}
	evt, messageType, dontRenderEdited, err := decodeStagedWAMessage(item.Payload)
	if err != nil {
		return nil, err
	}
	wrapped := &WAMessageEvent{
		MessageInfoWrapper: &MessageInfoWrapper{Info: evt.Info, wa: wa},
		Message:            evt.Message, MsgEvent: evt,
		parsedMessageType: messageType, dontRenderEdited: dontRenderEdited,
	}
	if item.StableID != "wa-incoming:"+string(wrapped.GetID()) {
		return nil, fmt.Errorf("WhatsApp incoming event identity changed")
	}
	return wrapped, nil
}
