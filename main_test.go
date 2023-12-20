package main

import (
	"fmt"
	"testing"

)

func TestIface2date(t *testing.T) {
	input := interface{}("2023-07-03T09:10:33.380+03:00")
	output, err := iface2date(input)
	if err != nil {
		t.Errorf("%s", err)
	}
	fmt.Println(output)
} 

// func TestString2uuid(t *testing.T) {
// 	string2uuid("adwdfawfawf")
// 	string2uuid("аофцуатфца")

// }