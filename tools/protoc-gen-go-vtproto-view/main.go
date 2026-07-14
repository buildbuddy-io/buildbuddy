// protoc-gen-go-vtproto-view is a protoc plugin that generates
// zero-allocation wire "views": plain structs holding just the fields
// annotated with a given (view.name) name (see proto/view.proto), plus an
// UnmarshalWire method that decodes only those fields from a serialized
// message and skips everything else in O(1) per field record.
//
// For each message M whose tree uses view name "SomeView" it generates:
//
//	type MSomeViewView struct {
//	    <ScalarField> <GoType>              // annotated scalar fields
//	    <MessageField> <SubMessage>SomeViewView // value-embedded, no alloc
//	    Has<MessageField> bool              // presence of the field record
//	}
//	func (v *MSomeViewView) UnmarshalWire(dAtA []byte) error
//
// View membership propagates: a message field is part of a view if it is
// annotated itself (recording presence) or if any field in its subtree is.
// Sub-views are embedded by value and mirror the message nesting, so parsing
// a view performs no heap allocations except copies of selected string
// fields. Bytes fields are zero-copy: they alias the input buffer and are
// only valid while it remains live and unmodified.
//
// The decode logic mirrors protoc-gen-go-vtproto's UnmarshalVT (this plugin
// builds on vtprotobuf's generator package), so acceptance behavior matches
// UnmarshalVT exactly: any buffer UnmarshalVT accepts is accepted, with
// identical values for the view's fields. Like UnmarshalVT, merge semantics
// apply within a buffer (last scalar occurrence wins, repeated message
// records merge); unlike UnmarshalVT, UnmarshalWire resets the view first,
// and fields outside the view are skipped without wire-type validation or
// unknown-field retention.
//
// Annotating an unsupported field is a build error: repeated fields, maps,
// oneofs, explicit-presence scalars, sint/fixed/float kinds, groups,
// extensions, message types from other generation runs, messages with proto2
// required fields, and message cycles are not supported.
package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"text/template"

	"github.com/planetscale/vtprotobuf/generator"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	viewpb "github.com/buildbuddy-io/buildbuddy/proto/view"
)

func main() {
	protogen.Options{}.Run(generate)
}

func generate(plugin *protogen.Plugin) error {
	plugin.SupportedFeatures = generator.SupportedFeatures
	local := make(map[protoreflect.FullName]bool)
	for _, f := range plugin.Files {
		if f.Generate {
			local[f.Desc.Package()] = true
		}
	}
	cfg := &generator.Config{
		Poolable:            generator.NewObjectSet(),
		PoolableExclude:     generator.NewObjectSet(),
		IgnoreUnknownFields: generator.NewObjectSet(),
	}
	for _, file := range plugin.Files {
		if !file.Generate {
			continue
		}
		gf := plugin.NewGeneratedFile(file.GeneratedFilenamePrefix+"_vtview.pb.go", file.GoImportPath)
		p := &viewGen{
			GeneratedFile: &generator.GeneratedFile{
				GeneratedFile: gf,
				Config:        cfg,
				LocalPackages: local,
			},
			views: make(map[*protogen.Message]map[string]bool),
		}
		ok, err := p.generateFile(file)
		if err != nil {
			return err
		}
		if !ok {
			gf.Skip()
		}
	}
	return nil
}

type viewGen struct {
	*generator.GeneratedFile
	// views memoizes the transitive view names used in each message's tree.
	views map[*protogen.Message]map[string]bool
	// visiting guards messageViews against message cycles.
	visiting map[*protogen.Message]bool
	once     bool
}

// D is the data passed to emission templates. Identifiers from other packages
// must be resolved through p.Ident/p.helper (which register the imports)
// before being placed in a D.
type D map[string]any

// render expands an emission template to a string. Generated files are parsed
// and gofmt'ed by protogen before being written, so template indentation is
// cosmetic and a malformed template fails the build at generation time.
func render(text string, data D) string {
	var buf bytes.Buffer
	if err := template.Must(template.New("emit").Parse(text)).Execute(&buf, data); err != nil {
		// Templates are static and the data is fully under our control, so
		// this is always a bug in this plugin.
		panic(err)
	}
	return strings.TrimSpace(buf.String())
}

// tmpl renders an emission template into the generated file, merging the
// common identifiers (p.common) into data.
func (p *viewGen) tmpl(text string, data D) {
	p.P(render(text, p.common(data)))
}

// common returns template data shared by the emission templates, with the
// call-specific entries of extra merged in.
func (p *viewGen) common(extra D) D {
	d := D{
		"Errorf":           p.Ident("fmt", "Errorf"),
		"EOF":              p.Ident("io", "ErrUnexpectedEOF"),
		"ErrIntOverflow":   p.helper("ErrIntOverflow"),
		"ErrInvalidLength": p.helper("ErrInvalidLength"),
		"Skip":             p.helper("Skip"),
		"EndGroupType":     int(protowire.EndGroupType),
	}
	for k, v := range extra {
		d[k] = v
	}
	return d
}

// helper resolves a vtprotobuf protohelpers identifier.
func (p *viewGen) helper(name string) string {
	return p.QualifiedGoIdent(p.Helper(name))
}

func (p *viewGen) generateFile(file *protogen.File) (bool, error) {
	p.tmpl(`
		// Code generated by protoc-gen-go-vtproto-view. DO NOT EDIT.
		// source: {{.Source}}

		package {{.Package}}
	`, D{"Source": file.Desc.Path(), "Package": file.GoPackageName})
	for _, message := range file.Messages {
		if err := p.message(message); err != nil {
			return false, err
		}
	}
	return p.once, nil
}

// fieldViews returns the view names a field is directly annotated with.
func fieldViews(field *protogen.Field) []string {
	return proto.GetExtension(field.Desc.Options(), viewpb.E_Name).([]string)
}

// messageViews returns the set of view names used anywhere in the message's
// tree: direct field annotations plus, transitively, annotations inside
// singular message-typed fields.
func (p *viewGen) messageViews(message *protogen.Message) (map[string]bool, error) {
	if v, ok := p.views[message]; ok {
		return v, nil
	}
	if p.visiting == nil {
		p.visiting = make(map[*protogen.Message]bool)
	}
	if p.visiting[message] {
		return nil, fmt.Errorf("message cycle through %s; views cannot span recursive messages", message.Desc.FullName())
	}
	p.visiting[message] = true
	defer delete(p.visiting, message)

	views := make(map[string]bool)
	for _, field := range message.Fields {
		for _, name := range fieldViews(field) {
			views[name] = true
		}
		if field.Message != nil && !field.Desc.IsList() && !field.Desc.IsMap() && field.Oneof == nil {
			sub, err := p.messageViews(field.Message)
			if err != nil {
				return nil, err
			}
			for name := range sub {
				views[name] = true
			}
		}
	}
	p.views[message] = views
	return views, nil
}

// viewFields returns the fields of message included in the named view.
func (p *viewGen) viewFields(message *protogen.Message, view string) ([]*protogen.Field, error) {
	var fields []*protogen.Field
	for _, field := range message.Fields {
		tagged := false
		for _, name := range fieldViews(field) {
			if !validViewName(name) {
				return nil, fmt.Errorf("field %s: invalid view name %q (must be an uppercase letter followed by letters and digits, e.g. \"FindMissing\")", field.Desc.FullName(), name)
			}
			if name == view {
				tagged = true
			}
		}
		include := tagged
		if !include && field.Message != nil && field.Oneof == nil && !field.Desc.IsList() && !field.Desc.IsMap() {
			sub, err := p.messageViews(field.Message)
			if err != nil {
				return nil, err
			}
			include = sub[view]
		}
		if !include {
			continue
		}
		if err := p.checkSupported(field); err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// validViewName reports whether name can be used verbatim in the exported Go
// type name of the generated view: an uppercase letter followed by letters
// and digits.
func validViewName(name string) bool {
	for i, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
		case i > 0 && (r >= 'a' && r <= 'z' || r >= '0' && r <= '9'):
		default:
			return false
		}
	}
	return name != ""
}

// checkSupported returns an error if a field included in a view cannot be
// generated. Unlike a silently-skipped unsupported field, an explicit
// annotation must be honored or fail the build.
func (p *viewGen) checkSupported(field *protogen.Field) error {
	if field.Desc.IsWeak() || field.Oneof != nil || field.Desc.IsList() || field.Desc.IsMap() {
		return fmt.Errorf("view field %s: repeated, map, oneof, and weak fields are not supported", field.Desc.FullName())
	}
	switch field.Desc.Kind() {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.StringKind, protoreflect.BytesKind:
		if field.Desc.HasPresence() {
			return fmt.Errorf("view field %s: explicit-presence scalars are not supported", field.Desc.FullName())
		}
		return nil
	case protoreflect.MessageKind:
		if field.Message.Desc.IsMapEntry() {
			return fmt.Errorf("view field %s: map fields are not supported", field.Desc.FullName())
		}
		if !p.IsLocalMessage(field.Message) {
			return fmt.Errorf("view field %s: message type %s is from a different generation run", field.Desc.FullName(), field.Message.Desc.FullName())
		}
		if field.Message.Desc.RequiredNumbers().Len() > 0 {
			return fmt.Errorf("view field %s: message types with required fields are not supported", field.Desc.FullName())
		}
		return nil
	default:
		return fmt.Errorf("view field %s: kind %v is not supported", field.Desc.FullName(), field.Desc.Kind())
	}
}

// viewTypeName returns e.g. "FileMetadataFindMissingView" for message
// FileMetadata and view "FindMissing".
func viewTypeName(message *protogen.Message, view string) string {
	return message.GoIdent.GoName + view + "View"
}

func (p *viewGen) message(message *protogen.Message) error {
	for _, nested := range message.Messages {
		if err := p.message(nested); err != nil {
			return err
		}
	}
	if message.Desc.IsMapEntry() {
		return nil
	}
	views, err := p.messageViews(message)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := p.view(message, name); err != nil {
			return err
		}
	}
	return nil
}

func (p *viewGen) view(message *protogen.Message, view string) error {
	fields, err := p.viewFields(message, view)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		// The view only touches this message's subtree via untagged
		// descendants of untagged fields; nothing to generate here.
		return nil
	}
	p.once = true
	typeName := viewTypeName(message, view)

	var decls []string
	for _, field := range fields {
		if field.Message != nil {
			sub, err := p.messageViews(field.Message)
			if err != nil {
				return err
			}
			if sub[view] {
				decls = append(decls, field.GoName+" "+viewTypeName(field.Message, view))
			}
			decls = append(decls, "Has"+field.GoName+" bool")
		} else {
			typ, _ := p.FieldGoType(field)
			decls = append(decls, field.GoName+" "+typ)
		}
	}

	p.tmpl(`
		// {{.View}} is the "{{.Name}}" view of {{.Message}}: a
		// projection holding only the fields annotated with [(view.name) = "{{.Name}}"],
		// decoded directly from the wire representation without unmarshalling the
		// full message. Message-typed fields are embedded by value along with a
		// Has* presence bool, so parsing allocates nothing beyond string copies.
		type {{.View}} struct {
		{{- range .Decls}}
			{{.}}
		{{- end}}
		}

		// UnmarshalWire resets v and decodes the view's fields from dAtA, a
		// serialized {{.Message}}. Any buffer UnmarshalVT accepts is decoded with
		// identical values for the view's fields; all other fields are skipped
		// without being validated or retained.
		//
		// Bytes fields are zero-copy: they alias dAtA rather than copying, so
		// they are only valid as long as dAtA remains live and unmodified.
		func (v *{{.View}}) UnmarshalWire(dAtA []byte) error {
			*v = {{.View}}{}
			return v.unmarshalWire(dAtA)
		}

		// unmarshalWire does not reset v, so repeated occurrences of a message field
		// within one buffer merge, matching UnmarshalVT semantics.
		func (v *{{.View}}) unmarshalWire(dAtA []byte) error {
			l := len(dAtA)
			iNdEx := 0
			for iNdEx < l {
				preIndex := iNdEx
				var wire uint64
				{{.DecodeTag}}
				fieldNum := int32(wire >> 3)
				wireType := int(wire & 0x7)
				if wireType == {{.EndGroupType}} {
					return {{.Errorf}}("proto: {{.Message}}: wiretype end group for non-group")
				}
				if fieldNum <= 0 {
					return {{.Errorf}}("proto: {{.Message}}: illegal tag %d (wire type %d)", fieldNum, wire)
				}
				switch fieldNum {
	`, D{
		"View":      typeName,
		"Name":      view,
		"Message":   message.GoIdent.GoName,
		"Decls":     decls,
		"DecodeTag": p.varint("wire", "uint64"),
	})

	for _, field := range fields {
		if err := p.fieldCase(view, field); err != nil {
			return err
		}
	}

	// Fields outside the view and unknown fields share the skip path,
	// which mirrors UnmarshalVT's default branch (minus unknown-field
	// retention). It stays inline rather than in a runtime helper because
	// the extra call costs ~10% on the FileMetadata benchmark.
	p.tmpl(`
				}
				iNdEx = preIndex
				skippy, err := {{.Skip}}(dAtA[iNdEx:])
				if err != nil {
					return err
				}
				if (skippy < 0) || (iNdEx+skippy) < 0 {
					return {{.ErrInvalidLength}}
				}
				if (iNdEx + skippy) > l {
					return {{.EOF}}
				}
				iNdEx += skippy
			}

			if iNdEx > l {
				return {{.EOF}}
			}
			return nil
		}
	`, nil)
	return nil
}

// fieldCase emits the case clause for one view field. Each decode ends with
// `continue`; falling out of the switch means "skip this record".
func (p *viewGen) fieldCase(view string, field *protogen.Field) error {
	body, err := p.fieldBody(view, field)
	if err != nil {
		return err
	}
	p.tmpl(`
		case {{.Num}}:
			if wireType != {{.WireType}} {
				return {{.Errorf}}("proto: wrong wireType = %d for field {{.Name}}", wireType)
			}
			{{.Body}}
			continue
	`, D{
		"Num":      int(field.Desc.Number()),
		"WireType": int(generator.ProtoWireType(field.Desc.Kind())),
		"Name":     field.GoName,
		"Body":     body,
	})
	return nil
}

func (p *viewGen) fieldBody(view string, field *protogen.Field) (string, error) {
	if field.Message != nil {
		sub, err := p.messageViews(field.Message)
		if err != nil {
			return "", err
		}
		return render(`
			{{.DecodeLen}}
			v.Has{{.Name}} = true
			{{- if .HasSubView}}
			if err := v.{{.Name}}.unmarshalWire(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			{{- end}}
			iNdEx = postIndex
		`, D{
			"DecodeLen":  p.lenPrefix(),
			"Name":       field.GoName,
			"HasSubView": sub[view],
		}), nil
	}

	name := field.GoName
	typ, _ := p.FieldGoType(field)
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		return render(`
			var vb int
			{{.Decode}}
			v.{{.Name}} = {{.Type}}(vb != 0)
		`, D{"Decode": p.varint("vb", "int"), "Name": name, "Type": typ}), nil
	case protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return render(`
			v.{{.Name}} = 0
			{{.Decode}}
		`, D{"Decode": p.varint("v."+name, typ), "Name": name}), nil
	case protoreflect.StringKind:
		return render(`
			{{.DecodeLen}}
			v.{{.Name}} = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		`, D{"DecodeLen": p.lenPrefix(), "Name": name}), nil
	case protoreflect.BytesKind:
		// Zero-copy: the field aliases dAtA rather than copying. An empty
		// record still yields a non-nil subslice, preserving the distinction
		// from an absent field (nil after reset).
		return render(`
			{{.DecodeLen}}
			v.{{.Name}} = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
		`, D{"DecodeLen": p.lenPrefix(), "Name": name}), nil
	default:
		return "", fmt.Errorf("view field %s: kind %v is not supported", field.Desc.FullName(), field.Desc.Kind())
	}
}

// varint renders an inline varint decode into target (which must already be
// declared), matching UnmarshalVT's generated decode exactly. It stays
// generated rather than in viewhelpers because inlining it is what makes
// views faster than a protowire-based parser.
func (p *viewGen) varint(target, typ string) string {
	return render(`
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return {{.ErrIntOverflow}}
			}
			if iNdEx >= l {
				return {{.EOF}}
			}
			b := dAtA[iNdEx]
			iNdEx++
			{{.Target}} |= {{.Type}}(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
	`, p.common(D{"Target": target, "Type": typ}))
}

// lenPrefix renders decoding of a LEN record's length, declaring postIndex.
func (p *viewGen) lenPrefix() string {
	return render(`
		var msglen int
		{{.Decode}}
		if msglen < 0 {
			return {{.ErrInvalidLength}}
		}
		postIndex := iNdEx + msglen
		if postIndex < 0 {
			return {{.ErrInvalidLength}}
		}
		if postIndex > l {
			return {{.EOF}}
		}
	`, p.common(D{"Decode": p.varint("msglen", "int")}))
}
