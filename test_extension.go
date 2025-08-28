package main

import (
	"fmt"
	"github.com/multigres/parser/go/parser"
)

func main() {
	sql := "CREATE EXTENSION postgis WITH SCHEMA myschema;"
	result, err := parser.Parse(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println(result.SqlString())
}
