package flag_form

// Form is an enum that describes the form the flag takes. The least-significant
// bit indicate whether it is a positive or negative flag, and the other bits
// indicate the type of the flag's name (currently standard or short).
type Form byte

// When adding a new form to this enum definition block, it should be added
// before `Short`, and the definition of its negative form should immediately
// follow it.
const (
	// The form of a flag using a standard name; for example: "--name"
	Standard Form = iota
	// The negative form of flag using a standard name; for example: "--noname"
	Negative

	// The short form of a flag; for example: "-x"
	Short

	// The form is unknown. We use the value after short because this would
	// normally be the negative of short form, but since short form cannot be
	// negative, this value is a convenient 'undefined'.
	//
	// Unknown must be the last value defined in this block.
	Unknown
)

// Mask to check if flag is negative form with '&' or negate flag with '|'
const negativeMask = Negative

// Negative returns whether or not this is a negative form. Unknown forms will
// always return false, as we cannot identify them as being in a negative form.
func (f Form) Negative() bool {
	return f < Unknown && f&negativeMask != 0
}

// SetNegative returns this form with the negative bit set. Returns Unknown if
// this is not a form for which the negative bit may be set.
func (f Form) SetNegative() Form {
	if f >= Unknown {
		return Unknown
	}
	return f | negativeMask
}

// Mask to clear the negative bit with '&'
const clearNegativeMask = ^negativeMask

// ClearNegative returns this form with negative bit cleared. Returns Unknown if
// this is not a form for which the negative bit may be cleared.
func (f Form) ClearNegative() Form {
	if f >= Unknown {
		return Unknown
	}
	return f & clearNegativeMask
}

// CompareNameType compares the name types only, ignoring the negative bit.
// Unknown forms will always return false, even if they have the same value,
// since we can't compare the types of unknown forms.
func (f Form) CompareNameType(f2 Form) bool {
	return !f.Unknown() && !f2.Unknown() && (f == f2 || (f.ClearNegative() == f2.ClearNegative()))
}

// AsNameType returns this form as a different name type. The negative bit from
// this form will be preserved in the returned Form.
func (f Form) AsNameType(f2 Form) Form {
	if f.Unknown() || f2.Unknown() {
		return Unknown
	}
	return (f & negativeMask) | (f2 & clearNegativeMask)
}

// Unknown returns whether or not this is an unknown form.
func (f Form) Unknown() bool {
	return f >= Unknown
}
