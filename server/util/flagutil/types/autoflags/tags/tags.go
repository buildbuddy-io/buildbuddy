package tags

import (
	"flag"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type Tagged interface {
	Value() flag.Value
	Zero() string

	// from flag.Value
	SetFunc(func(value string) error)
	StringFunc(func() string)

	// from WrappingValue
	WrappedValueFunc(func() flag.Value)

	// from Expandable
	ExpandFunc(func(mapping func(string) (string, error)) error )

	// from Secretable
	IsSecretFunc(func() bool)

	// from SetValueForFlagNameHooked
	SetValueForFlagNameHookFunc(func())

	// from YAMLSetValueHooked
	YAMLSetValueHookFunc(func())
}

type Taggable interface {
	Tag(flagset *flag.FlagSet, name string, f Tagged)
}

type TagFlag[T any] struct {
	value flag.Value

	setFunc func(value string) error
	stringFunc func() string
	wrappedValueFunc func() flag.Value
	expandFunc func(mapping func(string) (string, error)) error 
	isSecretFunc func() bool
	setValueForFlagNameHookFunc func()
	yamlSetValueHookFunc func()
}

func (t *TagFlag[T]) Value() flag.Value {
	return t.value
}

func (_ *TagFlag[T]) Zero() string {
	return fmt.Sprint(common.Zero[T]())
}

func (t *TagFlag[T]) SetFunc(setFunc func(string) error) {
	t.setFunc = setFunc
}

func (t *TagFlag[T]) Set(value string) error {
	if t.setFunc != nil {
		return t.setFunc(value)
	}
	return t.Value().Set(value)
}

func (t *TagFlag[T]) StringFunc(stringFunc func() string) {
	t.stringFunc = stringFunc
}

func (t *TagFlag[T]) String() string {
	if t == nil {
		return t.Zero()
	}
	if t.stringFunc != nil {
		return t.stringFunc()
	}
	if t.Value() == nil {
		return t.Zero()
	}
	return t.Value().String()
}

func (t *TagFlag[T]) WrappedValueFunc(wrappedValueFunc func() flag.Value) {
	t.wrappedValueFunc = wrappedValueFunc
}

func (t *TagFlag[T]) WrappedValue() flag.Value {
	if t.wrappedValueFunc != nil {
		return t.wrappedValueFunc()
	}
	return t.Value()
}

func (t *TagFlag[T]) ExpandFunc(expandFunc func(func(string) (string, error)) error) {
	t.expandFunc = expandFunc
}

func (t *TagFlag[T]) Expand(mapping func(string) (string, error)) error {
	if t.expandFunc != nil {
		return t.expandFunc(mapping)
	}
	return common.Expand(t.Value(), mapping)
}

func (t *TagFlag[T]) IsSecretFunc(isSecretFunc func() bool) {
	t.isSecretFunc = isSecretFunc
}

func (t *TagFlag[T]) IsSecret() bool {
	if t.isSecretFunc != nil {
		return t.isSecretFunc()
	}
	return false
}

func (t *TagFlag[T]) SetValueForFlagNameHookFunc(setValueForFlagNameHookFunc func()) {
	t.setValueForFlagNameHookFunc = setValueForFlagNameHookFunc
}

func (t *TagFlag[T]) SetValueForFlagNameHook() {
	if t.setValueForFlagNameHookFunc != nil {
		t.setValueForFlagNameHookFunc()
	}
}

func (t *TagFlag[T]) YAMLSetValueHookFunc(yamlSetValueHookFunc func()) {
	t.yamlSetValueHookFunc = yamlSetValueHookFunc
}

func (t *TagFlag[T]) YAMLSetValueHook() {
	if t.yamlSetValueHookFunc != nil {
		t.yamlSetValueHookFunc()
	}
}

// Tag wraps the flag specified by name in a TagFlag[T] and passes it to the
// Tag method of the passed Taggable.
func Tag[T any](flagset *flag.FlagSet, name string, t Taggable) {
	flg := flagset.Lookup(name)
	converted, err := common.ConvertFlagValue(flg.Value)
	if err != nil {
		log.Fatalf("Error creating TagFlag[%T] for flag %s: %v", common.Zero[T](), name, err)
	} else if _, ok := converted.(*T); !ok {
		log.Fatalf("Error creating TagFlag[%T] for flag %s: could not coerce flag of type %T to type %T.", common.Zero[T](), name, converted, (*T)(nil))
	}
	tf := &TagFlag[T]{
		value: flg.Value,
	}
	flg.Value = tf
	t.Tag(flagset, name, tf)
}

type deprecatedTag struct {
	migrationPlan string
}

func (d *deprecatedTag) Tag(flagset *flag.FlagSet, name string, tagged Tagged) {
	flg := flagset.Lookup(name)
	flg.Usage = flg.Usage + " **DEPRECATED** " + d.migrationPlan

	tagged.SetFunc(func(value string) error {
		log.Warningf(
			"Flag \"%s\" was set on the command line but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
		return tagged.Value().Set(value)
	})
	tagged.SetValueForFlagNameHookFunc(func() {
		log.Warningf(
			"Flag \"%s\" was set programmatically by name but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
	})
	tagged.YAMLSetValueHookFunc(func() {
		log.Warningf(
			"Flag \"%s\" was set through the YAML config but has been deprecated: %s",
			name,
			d.migrationPlan,
		)
	})
}

func DeprecatedTag(migrationPlan string) Taggable {
	return &deprecatedTag{migrationPlan: migrationPlan}
}


type secretTag struct{}

func (_ *secretTag) Tag(flagset *flag.FlagSet, name string, tagged Tagged) {
	tagged.IsSecretFunc(func() bool { return true })
}

var SecretTag = &secretTag{}
