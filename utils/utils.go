package utils

import "strings"

func RemovePassword(uri string) string {
	p := strings.LastIndex(uri, "@")
	if p < 0 {
		return uri
	}
	sp := strings.Index(uri, "://") + 3
	if sp == 2 {
		sp = 0
	}
	cp := strings.Index(uri[sp:], ":")
	if cp < 0 || sp+cp > p {
		return uri
	}
	return uri[:sp+cp] + ":****" + uri[p:]
}
