package main

import "regexp"

func main() {
	var wordPattern = regexp.MustCompile("[a-zA-Z]+\\.\\*|[a-zA-Z]+[.]\\w+")
	println(wordPattern.Match([]byte("abc.*a")))
}
