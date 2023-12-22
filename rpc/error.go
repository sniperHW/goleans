package rpc

import "errors"

var (
	ErrCallOK              = errors.New("call ok")
	ErrCallTimeout         = errors.New("call timeout")
	ErrCallCancel          = errors.New("context.Cancel")
	ErrCallMethodPanic     = errors.New("method call Panic")
	ErrCallInvaildArgument = errors.New("method call invaild argument")
	ErrCallMethodNotFound  = errors.New("method call method not found")
	ErrCallRedirect        = errors.New("grain placement Redirect")
	ErrCallRetry           = errors.New("retry again")
	ErrCallInvaildPid      = errors.New("invaild grain pid")
	ErrMailBoxClosed       = errors.New("mailbox closed")
	ErrMailBoxFull         = errors.New("mailbox full")
	ErrActivateFailed      = errors.New("activate failed")
)

const (
	ErrCodeOk = iota
	ErrCodeMethodCallPanic
	ErrCodeInvaildArg
	ErrCodeMethodNotExist
	ErrCodeRedirect
	ErrCodeRetryAgain
	ErrCodeInvaildPid
	ErrCodeActivateFailed
)

var errDesc []error = []error{
	ErrCallOK,
	ErrCallMethodPanic,
	ErrCallInvaildArgument,
	ErrCallMethodNotFound,
	ErrCallRedirect,
	ErrCallRetry,
	ErrCallInvaildPid,
	ErrActivateFailed,
}

func getDescByErrCode(code uint16) error {
	if int(code) < len(errDesc) {
		return errDesc[code]
	} else {
		return errors.New("unknow error")
	}
}
