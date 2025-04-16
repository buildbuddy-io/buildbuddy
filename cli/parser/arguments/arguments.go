package arguments

// Argument represents parsed command-line argument (or arguments, in the case
// of an `--option value` pair). This interface is implemented by
// `PositionalArgument` and `options.Option`. The main benefits this interface
// affords us are 1) being able to store `option.Options` and
// `PositionalArguments` in the same slice in a type-safe way, and 2) being
// able to easily `Format` a slice of `Argument`s in order to retrieve the
// string representation of the arguments.
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

func AsArguments[T Argument](args []T) []Argument {
	argSlice := make([]Argument, 0, len(args))
	for _, a := range args {
		// Obviously this looks wrong and one would want to just write
		// `argSlice = append(argSlice, args...)` instead of this verbose loop, but
		// `append(s, e)` is actually just syntactic sugar for `append(s, []E{e})`
		// (where `E` is the type of the element in `s`), which means that this
		// works because a `T` is being used as an `Argument` in a
		// newly-instantiated Argument slice, whereas `append(argSlice, args)`
		// would be attempting to satisfy the  second argument of the function
		// signature `append(s []Argument, els...Argument)` with a value of type
		// `[]T`, while `els` is of type []Argument. And while a `T` is an
		// `Argument`, a `[]T` is not a `[]Argument`. Hence, the loop.
		argSlice = append(argSlice, a)
	}
	return argSlice
}

func AsPositionalArguments(args []string) []Argument {
	pos := make([]Argument, 0, len(args))
	for _, arg := range args {
		pos = append(pos, &PositionalArgument{Value: arg})
	}
	return pos
}

func AsFormatted[T Argument](args []T) []string {
	s := make([]string, 0, len(args))
	for _, arg := range args {
		s = append(s, arg.Format()...)
	}
	return s
}

func AsValues[T Argument](args []T) []string {
	s := make([]string, 0, len(args))
	for _, arg := range args {
		s = append(s, arg.GetValue())
	}
	return s
}
