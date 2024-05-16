package tags

import (
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// Tagged exists as an interface to provide an abstraction of
// `TaggedFlagValue[T any, FV flag.Value]` that can allow a non-parameterized
//
//	struct implementing `Taggable` to modify the behavior of the methods of a
//
// `flag.Value` while still preserving its ability to return the correct string
// when `String()` is called on a zero-value of the flag.Value, which is only
// possible if the wrapped type is contained in a type parameter. This allows
// for the creation of flags using the following form:
//
// var foo = flag.Struct("foo", Foo{}, "A foo", flag.SecretTag)
//
// or:
//
// var foo = flag.Struct("foo", Foo{}, "A foo", flag.SecretTag())
//
// whereas otherwise we would be forced to use the form:
//
// var foo = flag.Struct("foo", Foo{}, "A foo", &flag.SecretTag[Foo]{})
//
// or:
//
// var foo = flag.Struct("foo", Foo{}, "A foo", flag.SecretTag[Foo]())
//
// which makes tags less readable / easy to use.
// This design also means that when new tags are created, they only need
// implement `Taggable` and, within that method, modify the functions that need
// to change, as opposed to needing to correctly implement, for example,
// `String()` (specifically in the zero case, especially) or `WrappedValue()`.
// "Designate" is used instead of the more common and less verbose "Set" to
// avoid awkward / potentially confusing names like `SetSetFunc`.
// `Value()` is provided to give access to the underlying value.
// `Zero()` is provided as a convenience function so that it is easier to handle
// the zero-value case when designating a new `String` func.
type Tagged interface {
	Value() flag.Value
	Zero() string

	Set(string) error
	String() string

	// from flag.Value
	DesignateSetFunc(func(value string) error)
	DesignateStringFunc(func() string)

	// from WrappingValue
	DesignateWrappedValueFunc(func() flag.Value)

	// from Expandable
	DesignateExpandFunc(func(mapping func(string) (string, error)) error)

	// from Secretable
	DesignateIsSecretFunc(func() bool)

	// from NameAliasable
	DesignateIsNameAliasingFunc(func() bool)
	DesignateAliasedNameFunc(func() string)

	// from SetValueForFlagNameHooked
	DesignateSetValueForFlagNameHookFunc(func())

	// from YAMLSetValueHooked
	DesignateYAMLSetValueHookFunc(func())
}

type Taggable interface {
	// Tag returns the new flag.Value that should be set to the flag at name,
	// or nil if no change should occur.
	Tag(flagset *flag.FlagSet, name string, f Tagged) flag.Value
}

// TaggedFlagValue exists to let us modify functions of a flag.Value in a `Tag`
// function.
// We need T and FV because flag types from the `flag` library are private, so
// to locate them we need to know it's a native flag type (denoted by FV being
// literally `flag.Value`) and we need the underlying type.
type TaggedFlagValue[T any, FV flag.Value] struct {
	value flag.Value

	setFunc                     func(value string) error
	stringFunc                  func() string
	wrappedValueFunc            func() flag.Value
	expandFunc                  func(mapping func(string) (string, error)) error
	isSecretFunc                func() bool
	isNameAliasingFunc          func() bool
	aliasedNameFunc             func() string
	setValueForFlagNameHookFunc func()
	yamlSetValueHookFunc        func()
}

func (t *TaggedFlagValue[T, FV]) Value() flag.Value {
	return t.value
}

func (_ *TaggedFlagValue[T, FV]) Zero() string {
	if reflect.TypeOf(common.Zero[FV]()) == reflect.TypeOf((flag.Value)(nil)) {
		if zero := common.ZeroFlagValueFromType[T](); zero != nil {
			return zero.String()
		}
		// fallback to the default string representation of T
		return fmt.Sprint(common.Zero[T]())
	}
	return reflect.New(reflect.TypeOf(common.Zero[FV]()).Elem()).Interface().(flag.Value).String()
}

func (t *TaggedFlagValue[T, FV]) DesignateSetFunc(setFunc func(string) error) {
	t.setFunc = setFunc
}

func (t *TaggedFlagValue[T, FV]) Set(value string) error {
	if t.setFunc != nil {
		return t.setFunc(value)
	}
	return t.WrappedValue().Set(value)
}

func (t *TaggedFlagValue[T, FV]) DesignateStringFunc(stringFunc func() string) {
	t.stringFunc = stringFunc
}

func (t *TaggedFlagValue[T, FV]) String() string {
	if t == nil {
		return t.Zero()
	}
	if t.stringFunc != nil {
		return t.stringFunc()
	}
	if t.WrappedValue() != nil {
		return t.WrappedValue().String()
	}
	return t.Zero()
}

func (t *TaggedFlagValue[T, FV]) DesignateWrappedValueFunc(wrappedValueFunc func() flag.Value) {
	t.wrappedValueFunc = wrappedValueFunc
}

func (t *TaggedFlagValue[T, FV]) WrappedValue() flag.Value {
	if t.wrappedValueFunc != nil {
		return t.wrappedValueFunc()
	}
	return t.Value()
}

func (t *TaggedFlagValue[T, FV]) DesignateExpandFunc(expandFunc func(func(string) (string, error)) error) {
	t.expandFunc = expandFunc
}

func (t *TaggedFlagValue[T, FV]) Expand(mapping func(string) (string, error)) error {
	if t.expandFunc != nil {
		return t.expandFunc(mapping)
	}
	return common.Expand(t.WrappedValue(), mapping)
}

func (t *TaggedFlagValue[T, FV]) DesignateIsSecretFunc(isSecretFunc func() bool) {
	t.isSecretFunc = isSecretFunc
}

func (t *TaggedFlagValue[T, FV]) IsSecret() bool {
	if t.isSecretFunc != nil {
		return t.isSecretFunc()
	}
	return false
}

func (t *TaggedFlagValue[T, FV]) DesignateIsNameAliasingFunc(isNameAliasingFunc func() bool) {
	t.isNameAliasingFunc = isNameAliasingFunc
}

func (t *TaggedFlagValue[T, FV]) IsNameAliasing() bool {
	if t.isNameAliasingFunc != nil {
		return t.isNameAliasingFunc()
	}
	return false
}

func (t *TaggedFlagValue[T, FV]) DesignateAliasedNameFunc(aliasedNameFunc func() string) {
	t.aliasedNameFunc = aliasedNameFunc
}

func (t *TaggedFlagValue[T, FV]) AliasedName() string {
	if t.aliasedNameFunc != nil {
		return t.aliasedNameFunc()
	}
	return ""
}

func (t *TaggedFlagValue[T, FV]) DesignateSetValueForFlagNameHookFunc(setValueForFlagNameHookFunc func()) {
	t.setValueForFlagNameHookFunc = setValueForFlagNameHookFunc
}

func (t *TaggedFlagValue[T, FV]) SetValueForFlagNameHook() {
	if t.setValueForFlagNameHookFunc != nil {
		t.setValueForFlagNameHookFunc()
	}
}

func (t *TaggedFlagValue[T, FV]) DesignateYAMLSetValueHookFunc(yamlSetValueHookFunc func()) {
	t.yamlSetValueHookFunc = yamlSetValueHookFunc
}

func (t *TaggedFlagValue[T, FV]) YAMLSetValueHook() {
	if t.yamlSetValueHookFunc != nil {
		t.yamlSetValueHookFunc()
	}
}

// Tag wraps the flag specified by name in a TaggedFlagValue[T] and passes it
// to the Tag method of the passed Taggable. It returns a pointer to the data
// the flag Value contains, converted to the same type as is returned when
// defining the flag initially
func Tag[T any, FV flag.Value](flagset *flag.FlagSet, name string, t Taggable) *T {
	flg := flagset.Lookup(name)
	converted, err := common.ConvertFlagValue(flg.Value)
	if err != nil {
		log.Fatalf("Error creating TaggedFlagValue[%T] when applying tag %v for flag %s: %v", common.Zero[T](), t, name, err)
	}
	data, ok := converted.(*T)
	if !ok {
		log.Fatalf("Error creating TaggedFlagValue[%T] for flag %s: could not coerce flag of type %T to type %T when applying tag %v.", common.Zero[T](), name, converted, (*T)(nil), t)
	}
	tf := &TaggedFlagValue[T, FV]{
		value: flg.Value,
	}
	if v := t.Tag(flagset, name, tf); v != nil {
		flg.Value = v
	}
	return data
}

type aliasTag struct {
	names []string
}

func (a *aliasTag) Tag(flagset *flag.FlagSet, name string, tagged Tagged) flag.Value {
	if len(a.names) == 0 {
		return nil
	}
	tagged.DesignateIsNameAliasingFunc(func() bool { return true })
	tagged.DesignateAliasedNameFunc(func() string { return name })
	tagged.DesignateSetFunc(func(value string) error { return flagset.Set(name, value) })

	tagged.DesignateWrappedValueFunc(func() flag.Value {
		if flagset == nil {
			return nil
		}
		flg := flagset.Lookup(name)
		if flg == nil {
			return nil
		}
		return flg.Value
	})

	var flg *flag.Flag
	for wrapper, ok := tagged.(common.WrappingValue); ok; wrapper, ok = wrapper.WrappedValue().(common.WrappingValue) {
		if aliaser, ok := wrapper.(common.NameAliasable); ok && aliaser.IsNameAliasing() {
			if flg = flagset.Lookup(aliaser.AliasedName()); flg == nil {
				log.Fatalf("Error aliasing flag %s as %s: flag %s does not exist.", name, strings.Join(a.names, ", "), aliaser.AliasedName())
			}
		}
	}
	for _, newName := range a.names {
		flagset.Var(tagged, newName, "Alias for "+name)
	}
	return nil
}

func AliasTag(names ...string) Taggable {
	return &aliasTag{names: names}
}

type deprecatedTag struct {
	migrationPlan string
}

func (d *deprecatedTag) Tag(flagset *flag.FlagSet, name string, tagged Tagged) flag.Value {
	flg := flagset.Lookup(name)
	flg.Usage = flg.Usage + " **DEPRECATED** " + d.migrationPlan

	tagged.DesignateSetFunc(func(value string) error {
		log.Warningf(
			"Flag \"%s\" was set on the command line but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
		return tagged.Value().Set(value)
	})
	tagged.DesignateSetValueForFlagNameHookFunc(func() {
		log.Warningf(
			"Flag \"%s\" was set programmatically by name but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
	})
	tagged.DesignateYAMLSetValueHookFunc(func() {
		log.Warningf(
			"Flag \"%s\" was set through the YAML config but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
	})
	return tagged
}

func DeprecatedTag(migrationPlan string) Taggable {
	return &deprecatedTag{migrationPlan: migrationPlan}
}

type secretTag struct{}

func (_ *secretTag) Tag(flagset *flag.FlagSet, name string, tagged Tagged) flag.Value {
	tagged.DesignateIsSecretFunc(func() bool { return true })
	return tagged
}

var SecretTag = &secretTag{}

type yamlIgnoreTag struct{}

func (_ *yamlIgnoreTag) Tag(flagset *flag.FlagSet, name string, f Tagged) flag.Value {
	flagyaml.IgnoreFlagForYAML(name)
	return nil
}

var YAMLIgnoreTag = &yamlIgnoreTag{}
