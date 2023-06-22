package utils

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var mvRe = regexp.MustCompile(`^(\d+).(\d+)$`)

// GetEnv gets value from environment variable or fallbacks to default value
// This snippet is from https://stackoverflow.com/a/40326580/3323419
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// ReplaceAccount replaces an endpoint account part with a new account value
func ReplaceAccount(account, path string, prefixes []string) string {
	parts := strings.Split(path, "/")
	for _, prefix := range prefixes {
		for i, part := range parts {
			if strings.HasPrefix(part, prefix) {
				parts[i] = prefix + account
				break
			}
		}
	}
	return strings.Join(parts, "/")
}

func CompareMicroversions(operator, want, have string) (bool, error) {
	if operator != "lte" && operator != "gte" {
		return false, fmt.Errorf("invalid microversions comparison %q operator, must be lte or gte", operator)
	}

	w, err := microversionToInt(want)
	if err != nil {
		return false, err
	}

	h, err := microversionToInt(have)
	if err != nil {
		return false, err
	}

	// lte
	if operator == "lte" {
		if w[0] < h[0] {
			return true, nil
		}

		return w[0] <= h[0] && w[1] <= h[1], nil
	}

	// gte
	if w[0] > h[0] {
		return true, nil
	}

	return w[0] >= h[0] && w[1] >= h[1], nil
}

func microversionToInt(mv string) ([]int, error) {
	res := mvRe.FindAllStringSubmatch(mv, -1)
	if len(res) == 1 && len(res[0]) == 3 {
		ver := res[0][1:]
		major, _ := strconv.Atoi(ver[0])
		minor, _ := strconv.Atoi(ver[1])
		return []int{
			major,
			minor,
		}, nil
	}
	return nil, fmt.Errorf("invalid microversion string: %v", mv)
}
