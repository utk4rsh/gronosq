package entry

import (
	"reflect"
	"testing"
)

func TestDefaultSchedulerEntry_Key(t *testing.T) {
	type fields struct {
		key     string
		payload string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"get_key_test", fields{payload: "payload", key: "key"}, "key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &DefaultSchedulerEntry{
				key:     tt.fields.key,
				payload: tt.fields.payload,
			}
			if got := s.Key(); got != tt.want {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultSchedulerEntry_Payload(t *testing.T) {
	type fields struct {
		key     string
		payload string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"get_payload_test", fields{payload: "payload", key: "key"}, "payload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &DefaultSchedulerEntry{
				key:     tt.fields.key,
				payload: tt.fields.payload,
			}
			if got := s.Payload(); got != tt.want {
				t.Errorf("Payload() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDefaultSchedulerEntry(t *testing.T) {
	type args struct {
		key     string
		payload string
	}
	tests := []struct {
		name string
		args args
		want *DefaultSchedulerEntry
	}{
		{"constructor_test", args{payload: "payload", key: "key"}, NewDefaultSchedulerEntry("key", "payload")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDefaultSchedulerEntry(tt.args.key, tt.args.payload); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDefaultSchedulerEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}
