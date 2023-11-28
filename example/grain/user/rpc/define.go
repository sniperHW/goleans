package main

type MethodDefine struct {
	Name  string
	Code  int32
	Proto string
}

var Defines []MethodDefine = []MethodDefine{
	{Name: "Test", Code: 1, Proto: "test.proto"},
	{Name: "Echo", Code: 2, Proto: "echo.proto"},
}
