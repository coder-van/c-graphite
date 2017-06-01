package receivers

type Receiver interface {
	Start()
	Stop()
}
