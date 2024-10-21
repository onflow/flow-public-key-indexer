package utils

import (
	"testing"
)

// Test data
var testData = []struct {
	account  string
	expected string
}{
	{
		account:  "0aceda6e9d212ffe7",
		expected: "0x0aceda6e9d212ffe7",
	},
	{
		account:  "002dbe0975051f24",
		expected: "0x002dbe0975051f24",
	},
	{
		account:  "0037ecb679d3981a8",
		expected: "0x0037ecb679d3981a8",
	},
	{
		account:  "1037ecb679d3981a8",
		expected: "0x1037ecb679d3981a8",
	},
	{
		account:  "00000d7919d03154",
		expected: "0x00000d7919d03154",
	},
}

// Test function to verify that leading zeros are preserved when processing account strings
func TestAdd0xPrefix(t *testing.T) {
	for _, tt := range testData {
		t.Run(tt.account, func(t *testing.T) {
			result := Add0xPrefix(tt.account)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestProcess0xPrefix(t *testing.T) {
	for _, tt := range testData {
		t.Run(tt.account, func(t *testing.T) {
			res := Add0xPrefix(tt.account)
			removed := Strip0xPrefix(res)
			result := Add0xPrefix(removed)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

var missLengthData = []struct {
	account  string
	expected string
}{
	{
		account:  "0xceda6e9d212ffe7",
		expected: "0x0ceda6e9d212ffe7",
	},
	{
		account:  "0x2dbe0975051f24",
		expected: "0x002dbe0975051f24",
	},
	{
		account:  "0x037ecb679d3981a8",
		expected: "0x037ecb679d3981a8",
	},
	{
		account:  "0x137ecb679d3981a8",
		expected: "0x137ecb679d3981a8",
	},
	{
		account:  "0x7484dd747449b46",
		expected: "0x07484dd747449b46",
	},
}

func TestFixAccountLength(t *testing.T) {
	for _, tt := range missLengthData {
		t.Run(tt.account, func(t *testing.T) {
			result := FixAccountLength(tt.account)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
