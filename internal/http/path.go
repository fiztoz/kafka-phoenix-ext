package http

import "strings"

// JoinPath joins an installation prefix with a route on the same host.
// A root prefix ("/" or "") contributes no extra slash, so JoinPath("/", "/wallboard")
// is "/wallboard" rather than the network-path reference "//wallboard".
// The root route itself stays "/".
func JoinPath(base, route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		route = "/"
	}
	if !strings.HasPrefix(route, "/") {
		route = "/" + route
	}
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" || base == "/" {
		return route
	}
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	if route == "/" {
		return base + "/"
	}
	return base + route
}
