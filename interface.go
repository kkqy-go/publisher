package publisher

type EventConfig struct {
	Key                        any
	AutoPublishToNewSubscriber bool
}
type EventWithConfig interface {
	Config() EventConfig
}
