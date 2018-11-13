package main

import (
    "fmt"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
    "github.com/tonyyanga/gdp-replicate/policy"
)

func main() {
    // Just to test import
    var msgType policy.MessageType
    msgType = 1

    var addr gdplogd.HashAddr
    addr = "abcdef"

    fmt.Println(msgType)
    fmt.Println(addr)
}

