package server

type QueryError struct {
	msg string
}

func NewQueryError(msg string) *QueryError {
	return &QueryError{msg: msg}
}

func (e *QueryError) Error() string {
	return e.msg
}
