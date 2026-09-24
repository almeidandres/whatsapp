package connector

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
)

// An on-demand end type with more accessible messages must never end pagination.
func TestHistoryAvailableOnPhone(t *testing.T) {
	if historyAvailableOnPhone(nil) || phoneHistoryFinished(nil) {
		t.Fatal("missing end type must not imply phone-history completion")
	}
	for _, tc := range []struct {
		end      waHistorySync.Conversation_EndOfHistoryTransferType
		more     bool
		complete bool
	}{
		{waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY, true, false},
		{waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY, false, true},
		{waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY, true, false},
		{waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS, false, false},
	} {
		if got := historyAvailableOnPhone(&tc.end); got != tc.more {
			t.Errorf("end type %d: accessible phone history = %t, want %t", tc.end, got, tc.more)
		}
		if got := phoneHistoryFinished(&tc.end); got != tc.complete {
			t.Errorf("end type %d: finished phone history = %t, want %t", tc.end, got, tc.complete)
		}
	}
}
