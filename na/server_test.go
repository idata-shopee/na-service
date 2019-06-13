package na

import (
	"strings"
	"testing"
	"time"
)

func TestParseProxyCallExp(t *testing.T) {
	st, fn, ps, to, err := ParseProxyCallExp([]interface{}{"user-service", []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, st, "user-service", "")
	assertEqual(t, fn, "getUser", "")
	assertEqual(t, len(ps), 1, "")
	assertEqual(t, ps[0], "test", "")
	assertEqual(t, err, nil, "")
	assertEqual(t, to, time.Duration(120)*time.Second, "")
}

func TestParseProxyCallExpError(t *testing.T) {
	_, _, _, _, err := ParseProxyCallExp([]interface{}{123, []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, strings.Index(err.Error(), "proxy") != -1, true, "")
}

func TestParseProxyStreamCallExp(t *testing.T) {
	st, fn, ps, to, err := ParseProxyStreamCallExp([]interface{}{"user-service", []interface{}{[]interface{}{"getUser", "test"}}, 120.0})
	assertEqual(t, st, "user-service", "")
	assertEqual(t, fn, "getUser", "")
	assertEqual(t, len(ps), 1, "")
	assertEqual(t, ps[0], "test", "")
	assertEqual(t, err, nil, "")
	assertEqual(t, to, time.Duration(120)*time.Second, "")
}
