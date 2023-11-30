package main

type MethodDefine struct {
	Code  uint16
	Proto string
}

var Defines []MethodDefine = []MethodDefine{
	{Code: 1, Proto: "test.proto"},
	{Code: 2, Proto: "echo.proto"},
	{Code: 3, Proto: "login.proto"},
	{Code: 4, Proto: "relaymsg.proto"},
	{Code: 5, Proto: "logout.proto"},
}

func init() {
	m := map[uint16]bool{}
	for _, v := range Defines {
		if m[v.Code] {
			panic("duplicate code")
		}
		m[v.Code] = true
	}
}
