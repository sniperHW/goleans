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
	"github.com/sniperHW/goleans"
	"github.com/sniperHW/goleans/pd"
	"time"
)

type Replyer struct {
	replyer *goleans.Replyer
}

func (r *Replyer) Reply(result *{{.Response}}) {
	r.replyer.Reply(result)
}

type {{.Service}} interface {
	Serve{{.Service}}(context.Context, *Replyer,*{{.Request}})
}

func Register(grain *goleans.Grain,o {{.Service}}) error {
	return grain.RegisterMethod({{.Method}}, func(ctx context.Context, r *goleans.Replyer, arg *{{.Request}}){
		o.Serve{{.Service}}(ctx,&Replyer{replyer:r},arg)
	})	
}


func Call(ctx context.Context,pid pd.Pid,arg *{{.Request}}) (*{{.Response}},error) {
	var resp {{.Response}}
	err := goleans.Call(ctx,pid,{{.Method}},arg,&resp)
	return &resp,err
}

func CallWithTimeout(pid pd.Pid,arg *{{.Request}},d time.Duration) (*{{.Response}},error) {
	var resp {{.Response}}
	err := goleans.CallWithTimeout(pid,{{.Method}},arg,&resp,d)
	return &resp,err
}

`

type method struct {
	Name     string
	Method   uint16
	Request  string
	Response string
	Service  string
}

var (
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

	err = tmpl.Execute(f, method{
		Name:     name,
		Method:   code,
		Request:  fmt.Sprintf("%sReq", strings.Title(name)),
		Response: fmt.Sprintf("%sRsp", strings.Title(name)),
		Service:  strings.Title(name),
	})
	if err != nil {
		panic(err)
	} else {
		log.Printf("%s Write ok\n", filename)
	}
}

func main() {

	//inputPath = flag.String("inputPath", "proto", "inputPath")
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
