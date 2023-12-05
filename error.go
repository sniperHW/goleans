package goleans

import "errors"

var (
	ErrCallOK               = errors.New("call ok")
	ErrCallTimeout          = errors.New("call timeout")
	ErrCallCancel           = errors.New("context.Cancel")
	ErrCallMethodPanic      = errors.New("method call Panic")
	ErrCallInvaildArgument  = errors.New("method call invaild argument")
	ErrCallMethodNotFound   = errors.New("method call method not found")
	ErrCallGrainCreate      = errors.New("user Grain Create Error")
	ErrCallGrainInit        = errors.New("user Grain Init Error")
	ErrCallRedirect         = errors.New("grain placement Redirect")
	ErrCallRetry            = errors.New("retry again")
	ErrCallInvaildIdentity  = errors.New("invaild grain identity")
	ErrMailBoxClosed        = errors.New("mailbox closed")
	ErrMailBoxFull          = errors.New("mailbox full")
	ErrInitUnRetryAbleError = errors.New("unretryable error")
)
