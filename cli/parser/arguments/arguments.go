package arguments

type Argument interface {
	GetValue() string
	SetValue(value string)
	Format() []string
}

type PositionalArgument struct {
	Value string
}

func (a *PositionalArgument) GetValue() string {
	return a.Value
}

func (a *PositionalArgument) SetValue(value string) {
	a.Value = value
}

func (a *PositionalArgument) Format() []string {
	return []string{a.Value}
}

func AsFormatted[T Argument](args []T) []string {
	s := make([]string, 0, len(args))
	for _, arg := range args {
		s = append(s, arg.Format()...)
	}
	return s
}
