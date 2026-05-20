package strategy

import "fmt"

// Factory builds a Strategy from configuration. Each registered strategy provides one.
type Factory func() (Strategy, error)

var registry = map[string]Factory{}

// Register adds a strategy factory under the given name. Panics if the name is already taken;
// strategy registration should happen at package init, so a collision is a programming error.
func Register(name string, factory Factory) {
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("strategy already registered: %s", name))
	}
	registry[name] = factory
}

// Build resolves the strategy identified by name.
func Build(name string) (Strategy, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown STRATEGY: %s", name)
	}
	return factory()
}
