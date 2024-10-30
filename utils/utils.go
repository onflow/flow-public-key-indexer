package utils

import "strings"

func Strip0xPrefix(str string) string {
	if strings.HasPrefix(str, "0x") {
		return str[2:]
	}
	return str
}

func Add0xPrefix(s string) string {
	if !strings.HasPrefix(s, "0x") {
		return "0x" + s
	}
	return s
}

func FixAccountLength(account string) string {
	if len(account) == 18 {
		return account
	}
	stripped := Strip0xPrefix(account)

	// Add leading zeros
	for len(stripped) < 16 {
		stripped = "0" + stripped
	}

	return Add0xPrefix(stripped)
}
