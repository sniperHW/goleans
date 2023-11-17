package goleans

type Grain interface {
	GetIdentity() string
	RegisterMethod(uint16, interface{}) error
	GetMethod(uint16) *methodCaller
}

type GrainBase struct {
	mailbox  *Mailbox
	Identity string
	methods  map[uint16]*methodCaller
}

func (grain *GrainBase) GetIdentity() string {
	return grain.Identity
}

func (grain *GrainBase) GetMethod(method uint16) *methodCaller {
	return grain.methods[method]
}

func (grain *GrainBase) Init(mailBoxOpt MailboxOption) {
	grain.methods = map[uint16]*methodCaller{}
	grain.mailbox = &Mailbox{
		taskQueue:  make(chan func(), mailBoxOpt.TaskQueueCap),
		awakeQueue: make(chan *goroutine, 64),
		die:        make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
}

func (grain *GrainBase) RegisterMethod(method uint16, fn interface{}) error {
	if caller, err := makeMethodCaller(fn); err != nil {
		return err
	} else {
		grain.methods[method] = caller
		return nil
	}
}
