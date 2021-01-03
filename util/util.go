package util

import "bytes"

// return a string like a.b.c
func BuildDotString(strings ...string) string {
	bf := bytes.Buffer{}
	for _, str := range strings {
		if str == "" {
			continue
		}
		bf.WriteString(str)
		bf.WriteString(".")
	}
	ret := bf.String()
	loc := len(ret)
	for loc >= 1 && loc-1 < len(ret) && ret[loc-1] == '.' {
		loc--
	}
	return ret[0:loc]
}
