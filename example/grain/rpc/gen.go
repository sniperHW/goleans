package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"strings"
)

var templateStr string = `
package {{.Name}}

import (
	"context"
	"goleans"
	"goleans/pd"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *Response) {
	r.replyer.Reply(result)
}


func Register(grain *goleans.Grain,fn func(context.Context, *Replyer,*Request)) error {
	return grain.RegisterMethod({{.Method}}, func(ctx context.Context, r *goleans.Replyer, arg *Request){
		fn(ctx,&Replyer{replyer:r},arg)
	})
}

func Call(ctx context.Context,identity pd.GrainIdentity,arg *Request) (*Response,error) {
	var resp Response
	err := goleans.Call(ctx,identity,{{.Method}},arg,&resp)
	return &resp,err
}
`

type method struct {
	Name   string
	Method uint16
}

var (
	inputPath  *string
	outputPath *string
)

func gen(tmpl *template.Template, name string, code uint16) {
	filename := fmt.Sprintf("%s/%s/%s.go", *outputPath, name, name)
	os.MkdirAll(fmt.Sprintf("%s/%s", *outputPath, name), os.ModePerm)
	f, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
	if err != nil {
		if os.IsNotExist(err) {
			f, err = os.Create(filename)
			if err != nil {
				log.Printf("------ error -------- create %s failed:%s", filename, err.Error())
				return
			}
		} else {
			log.Printf("------ error -------- open %s failed:%s", filename, err.Error())
			return
		}
	}
	defer f.Close()

	err = os.Truncate(filename, 0)
	if err != nil {
		log.Printf("------ error -------- Truncate %s failed:%s", filename, err.Error())
		return
	}

	err = tmpl.Execute(f, method{name, code})
	if err != nil {
		panic(err)
	} else {
		log.Printf("%s Write ok\n", filename)
	}
}

func main() {

	inputPath = flag.String("inputPath", "proto", "inputPath")
	outputPath = flag.String("outputPath", "service", "outputPath")

	flag.Parse()

	tmpl, err := template.New("test").Parse(templateStr)
	if err != nil {
		panic(err)
	}

	for _, v := range Defines {
		t := strings.Split(v.Proto, ".")
		if len(t) == 2 && t[1] == "proto" {
			gen(tmpl, t[0], uint16(v.Code))
		}
	}
}
