package main

import (
	"testing"

	"github.com/vpal/s3fastls/pkg/s3fastls"
)

func TestParseField(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    s3fastls.Field
		wantErr bool
	}{
		{"Key", "Key", s3fastls.FieldKey, false},
		{"Size", "Size", s3fastls.FieldSize, false},
		{"LastModified", "LastModified", s3fastls.FieldLastModified, false},
		{"ETag", "ETag", s3fastls.FieldETag, false},
		{"StorageClass", "StorageClass", s3fastls.FieldStorageClass, false},
		{"Invalid", "InvalidField", "", true},
		{"Empty", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseField(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseField() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFieldsFlag(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []s3fastls.Field
		wantErr bool
	}{
		{
			"Single field",
			"Key",
			[]s3fastls.Field{s3fastls.FieldKey},
			false,
		},
		{
			"Multiple fields",
			"Key,Size,LastModified",
			[]s3fastls.Field{s3fastls.FieldKey, s3fastls.FieldSize, s3fastls.FieldLastModified},
			false,
		},
		{
			"Empty",
			"",
			nil,
			true,
		},
		{
			"Invalid field",
			"Key,Invalid,Size",
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f FieldsFlag
			err := f.Set(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("FieldsFlag.Set() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(f) != len(tt.want) {
					t.Errorf("FieldsFlag.Set() len = %v, want %v", len(f), len(tt.want))
					return
				}
				for i := range f {
					if f[i] != tt.want[i] {
						t.Errorf("FieldsFlag.Set()[%d] = %v, want %v", i, f[i], tt.want[i])
					}
				}
			}
		})
	}
}
