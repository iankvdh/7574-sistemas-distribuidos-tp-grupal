package strategy

import "fmt"

type Factory func() (Strategy, error)

var registry = map[string]Factory{}

// Register installs a strategy factory under name. Panics on collision since
// registration happens at package init.
func Register(name string, factory Factory) {
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("strategy already registered: %s", name))
	}
	registry[name] = factory
}

func Build(name string) (Strategy, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown STRATEGY: %s", name)
	}
	return factory()
}
