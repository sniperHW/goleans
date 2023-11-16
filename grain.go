package actorgo

import "sync"

type Grain interface {
	GetIdentity() string
	RegisterMethod(uint16, interface{}) error
}

type GrainMgr struct {
	sync.RWMutex
	grains map[string]*Grain
}

var grainMgr GrainMgr = GrainMgr{grains: map[string]*Grain{}}

type GrainBase struct {
	Mailbox
	Identity string
	methods  map[uint16]*methodCaller
}

func (grain *GrainBase) InitMailbox(mailBoxOpt MailboxOption) {
	grain.Mailbox = Mailbox{
		taskQueue:  make(chan func(), mailBoxOpt.TaskQueueCap),
		awakeQueue: make(chan *goroutine, 64),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
}
