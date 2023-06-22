package utils

import (
	"testing"
)

func TestReplaceAccount(t *testing.T) {
	tests := []struct {
		name     string
		account  string
		endpoint string
		prefixes []string
		expected string
	}{
		{
			name:     "endpoint with account and existing prefix",
			account:  "john-doe",
			endpoint: "https://swift.openstack.domain.com:443/swift/v1/AUTH_randomstring1234",
			prefixes: []string{"AUTH_", "SERVICE_"},
			expected: "https://swift.openstack.domain.com:443/swift/v1/AUTH_john-doe",
		},
		{
			name:     "endpoint with account and non-existing prefix",
			account:  "john-doe",
			endpoint: "https://swift.openstack.domain.com:443/swift/v1/AUTH_randomstring1234",
			prefixes: []string{"IMAGE_"},
			expected: "https://swift.openstack.domain.com:443/swift/v1/AUTH_randomstring1234",
		},
		{
			name:     "endpoint without scheme, with account and existing prefix",
			account:  "john-doe",
			endpoint: "swift.domain.com:443/swift/v1/SERVICE_randomstring1234",
			prefixes: []string{"SERVICE_"},
			expected: "swift.domain.com:443/swift/v1/SERVICE_john-doe",
		},
	}
	for _, tt := range tests {
		path := ReplaceAccount(tt.account, tt.endpoint, tt.prefixes)
		if path != tt.expected {
			t.Errorf("[%s] failed - output %s doesn't match expected '%s'", tt.name, path, tt.expected)
		}
	}
}

func TestCompareMicroversions(t *testing.T) {
	type vals struct {
		want string
		op   string
		have string
		res  bool
	}
	tests := []vals{
		{
			"2.7", "lte", "2.50", true,
		},
		{
			"1.7", "lte", "2.50", true,
		},
		{
			"3.7", "lte", "2.50", false,
		},
		{
			"2.50", "lte", "2.50", true,
		},
		{
			"2.7", "gte", "2.50", false,
		},
		{
			"1.50", "gte", "2.50", false,
		},
		{
			"2.50", "gte", "2.50", true,
		},
	}

	for i, test := range tests {
		if v, err := CompareMicroversions(test.op, test.want, test.have); err != nil {
			t.Errorf("[%d] test failed: %v", i, err)
		} else if test.res != v {
			t.Errorf("[%d] test failed: expected %t, got %t", i, test.res, v)
		}
	}
}
