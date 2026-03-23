// mcptoolgen parses the API protos and outputs JSON schema for the subset of
// RPCs we want to expose as MCP tools.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/mcptools"
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
)

const (
	// apiV1ProtoRoot is the workspace-relative proto subtree imported by this
	// generator to resolve RPC, message, and field comments from source.
	//
	// This is *only* used for comments (tool descriptions). The JSON schema is
	// otherwise generated from the Go service definitions which is a bit
	// simpler than traversing proto descriptors.
	apiV1ProtoRoot = "proto/api/v1"

	// readOnlyRPCPrefix marks the RPC naming convention we currently use to
	// infer MCP's readOnlyHint metadata.
	readOnlyRPCPrefix = "Get"
)

var (
	// Prefix for request-message boilerplate that doesn't add anything useful
	// to the generated JSON schema description.
	requestMessageCommentPrefixPattern = regexp.MustCompile(`^Request passed into \w+\.?\s*`)

	// Suffix for "Next tag" comments which aren't useful for tool descriptions.
	protoCommentMetadataSuffixPattern = regexp.MustCompile(`(?m)^\s*Next tag:\s*\d+\s*$`)
)

// main runs the generator and reports one fatal error on stderr if generation
// fails.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses flags, builds the manifest, and writes the generated JSON file.
//
// The output is intentionally a single self-contained JSON blob so the MCP
// server can embed it directly without needing descriptor reflection at
// runtime.
func run() error {
	outputPath := flag.String("output", "", "Output path for the generated MCP tool manifest.")
	flag.Parse()
	if *outputPath == "" {
		return fmt.Errorf("-output is required")
	}
	manifest, err := buildManifest()
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*outputPath, data, 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// buildManifest walks the ApiService descriptor and emits one MCP tool entry
// for each reviewed unary RPC that is allowed to surface through MCP.
//
// Only ApiService methods are considered, and only the ones listed in
// [mcptools.AllowsRPC].
func buildManifest() (*mcptools.Manifest, error) {
	docs, err := loadProtoDocs()
	if err != nil {
		return nil, fmt.Errorf("load proto docs: %w", err)
	}
	service := apipb.File_proto_api_v1_service_proto.Services().ByName("ApiService")
	if service == nil {
		return nil, fmt.Errorf("api service descriptor not found")
	}
	tools := make([]mcptools.Tool, 0, service.Methods().Len())
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		// MCP tools are modeled as single request/response calls, so streaming
		// APIs do not fit this adapter.
		if method.IsStreamingClient() || method.IsStreamingServer() {
			continue
		}
		methodName := string(method.Name())
		// The allowlist is the source of truth for which public APIs we
		// intentionally surface to agents.
		if !mcptools.AllowsRPC(methodName) {
			continue
		}
		schema, err := (&schemaBuilder{
			docs: docs,
		}).SchemaForMessage(method.Input())
		if err != nil {
			return nil, fmt.Errorf("generate schema for %s: %w", method.Name(), err)
		}
		tools = append(tools, mcptools.Tool{
			Name:         toSnakeCase(methodName),
			RPCName:      methodName,
			RequestType:  string(method.Input().FullName()),
			ResponseType: string(method.Output().FullName()),
			Description:  docs.methodComment(method),
			ReadOnly:     strings.HasPrefix(methodName, readOnlyRPCPrefix),
			InputSchema:  schema,
		})
	}
	return &mcptools.Manifest{Tools: tools}, nil
}

// loadProtoDocs parses the checked-in api/v1 proto sources with source info so
// RPC and field comments can be copied into the generated manifest.
//
// The runtime MCP server only needs the final JSON manifest, so this is the
// one place where we pay the cost of reading proto sources directly.
func loadProtoDocs() (*protoDocs, error) {
	protoFiles, err := listProtoFiles(apiV1ProtoRoot)
	if err != nil {
		return nil, fmt.Errorf("list proto files: %w", err)
	}
	parser := protoparse.Parser{
		Accessor:              openProtoRunfile,
		IncludeSourceCodeInfo: true,
	}
	files, err := parser.ParseFilesButDoNotLink(protoFiles...)
	if err != nil {
		return nil, err
	}
	docs := newProtoDocs()
	for _, file := range files {
		docs.indexFileProto(file)
	}
	return docs, nil
}

// listProtoFiles walks one proto source subtree and returns every .proto file
// beneath it as a workspace-relative path.
func listProtoFiles(root string) ([]string, error) {
	runfilesRoot, runfilesFS, err := resolveProtoRoot(root)
	if err != nil {
		return nil, err
	}
	var files []string
	if runfilesFS != nil {
		if err := fs.WalkDir(runfilesFS, runfilesRoot, func(filePath string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || path.Ext(filePath) != ".proto" {
				return nil
			}
			relativePath, err := relativeRunfilesPath(runfilesRoot, filePath)
			if err != nil {
				return err
			}
			files = append(files, path.Join(root, relativePath))
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := filepath.WalkDir(runfilesRoot, func(filePath string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(filePath) != ".proto" {
				return nil
			}
			relativePath, err := filepath.Rel(runfilesRoot, filePath)
			if err != nil {
				return fmt.Errorf("rel %q: %w", filePath, err)
			}
			files = append(files, path.Join(root, filepath.ToSlash(relativePath)))
			return nil
		}); err != nil {
			return nil, err
		}
	}
	sort.Strings(files)
	return files, nil
}

// resolveProtoRoot finds the filesystem directory containing one workspace
// proto subtree, preferring Bazel runfiles and falling back to the local
// workspace path used by `bb run`.
func resolveProtoRoot(root string) (string, fs.FS, error) {
	if runfilesFS, err := runfiles.New(); err == nil {
		runfilesRoot := repoScopedRunfilePath(root)
		if info, statErr := fs.Stat(runfilesFS, runfilesRoot); statErr == nil && info.IsDir() {
			return runfilesRoot, runfilesFS, nil
		}
	}
	if info, err := os.Stat(root); err == nil && info.IsDir() {
		return root, nil, nil
	}
	return "", nil, fmt.Errorf("find proto root %q", root)
}

// openProtoRunfile resolves one checked-in proto source from Bazel runfiles.
//
// When the generator runs under `bb run`, opening the workspace-relative path
// directly also works, so we keep that as a fallback.
func openProtoRunfile(filename string) (io.ReadCloser, error) {
	resolvedPath, err := runfiles.Rlocation(repoScopedRunfilePath(filename))
	if err == nil {
		if file, openErr := os.Open(resolvedPath); openErr == nil {
			return file, nil
		}
	}
	return os.Open(filename)
}

// repoScopedRunfilePath prefixes a workspace-relative path with the canonical
// Bazel repository name when this generator runs from an external repository.
func repoScopedRunfilePath(pathInRepo string) string {
	if repo := runfiles.CurrentRepository(); repo != "" {
		return path.Join(repo, pathInRepo)
	}
	return pathInRepo
}

// relativeRunfilesPath strips one runfiles root prefix from a slash-separated
// path returned while walking a runfiles fs.FS.
func relativeRunfilesPath(root, filePath string) (string, error) {
	root = strings.TrimSuffix(root, "/")
	prefix := root + "/"
	if !strings.HasPrefix(filePath, prefix) {
		return "", fmt.Errorf("rel %q: missing prefix %q", filePath, prefix)
	}
	return strings.TrimPrefix(filePath, prefix), nil
}

// toSnakeCase converts a Go-style RPC name into the MCP tool naming convention.
//
// We keep tool names stable and predictable by deriving them directly from the
// protobuf RPC name instead of relying on hand-written aliases.
func toSnakeCase(s string) string {
	var out []rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			var next rune
			for _, next = range s[i+1:] {
				break
			}
			if i > 0 && shouldInsertUnderscore(rune(s[i-1]), r, next) {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

// shouldInsertUnderscore keeps acronym-heavy RPC names readable when converting
// them to snake_case.
//
// For example, this preserves `GetURL` as `get_url` instead of `get_u_r_l`
// while still splitting transitions like `GetInvocation` into
// `get_invocation`.
func shouldInsertUnderscore(prev, current, next rune) bool {
	if prev == '_' || !unicode.IsUpper(current) {
		return false
	}
	if unicode.IsLower(prev) || unicode.IsDigit(prev) {
		return true
	}
	return next != 0 && unicode.IsLower(next)
}

// schemaBuilder renders request messages as inline JSON Schema objects.
//
// The generator emits only input schemas because MCP tool calls send arguments
// from client to server. Response typing is still included in the manifest as
// metadata so the runtime can marshal the corresponding proto response back to
// JSON, but the client-facing schema is only for the request shape.
type schemaBuilder struct {
	docs *protoDocs
}

// SchemaForMessage returns the top-level JSON schema for one request message.
//
// We don't use $defs/$ref since it seems to have inconsistent support. Instead,
// we inline sub-message schemas.
//
// TODO: Use $defs/$ref once Codex correctly handles MCP schemas containing
// referenced subschemas: https://github.com/openai/codex/issues/3152
// This issue just mentions that $defs are treated as strings in the "list"
// call, and in practice Codex keeps fumbling with these $defs because it
// initially tries creating stringified JSON objects instead of using JSON
// objects directly.
// e.g. it will try to pass "commit_sha=abcdef" instead of {"commitSha": "abcdef"}
func (b *schemaBuilder) SchemaForMessage(desc protoreflect.MessageDescriptor) (json.RawMessage, error) {
	root := b.objectSchema(desc)
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// objectSchema renders one protobuf message as a closed JSON object schema.
//
// We disable additional properties so tool arguments stay close to the proto
// contract and typos are rejected by clients that validate against the schema.
func (b *schemaBuilder) objectSchema(desc protoreflect.MessageDescriptor) map[string]any {
	properties := make(map[string]any, desc.Fields().Len())
	for i := range desc.Fields().Len() {
		field := desc.Fields().Get(i)
		properties[field.JSONName()] = b.fieldSchema(field)
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if description := b.docs.messageComment(desc); description != "" {
		schema["description"] = description
	}
	return schema
}

// fieldSchema applies repeated and map container semantics around a field's
// underlying value schema.
//
// This keeps the scalar/message mapping logic centralized in valueSchema while
// letting container fields wrap that shape in the JSON representation expected
// by MCP clients.
func (b *schemaBuilder) fieldSchema(field protoreflect.FieldDescriptor) any {
	var schema any
	if field.IsMap() {
		schema = map[string]any{
			"type":                 "object",
			"additionalProperties": b.valueSchema(field.MapValue()),
		}
	} else {
		valueSchema := b.valueSchema(field)
		if field.Cardinality() == protoreflect.Repeated {
			schema = map[string]any{
				"type":  "array",
				"items": valueSchema,
			}
		} else {
			schema = valueSchema
		}
	}
	if description := b.docs.fieldComment(field); description != "" {
		return addSchemaDescription(schema, description)
	}
	return schema
}

// addSchemaDescription attaches a human-readable description to an existing
// JSON Schema fragment.
//
// Most schema fragments are objects already, but unconstrained values may be
// represented as the boolean schema `true`. In that case we convert the schema
// to an open object so the description is still preserved in the generated
// manifest.
func addSchemaDescription(schema any, description string) any {
	if description == "" {
		return schema
	}
	if schemaObject, ok := schema.(map[string]any); ok {
		if existingDescription, ok := schemaObject["description"].(string); ok && existingDescription != "" && existingDescription != description {
			schemaObject["description"] = description + "\n\n" + existingDescription
			return schemaObject
		}
		schemaObject["description"] = description
		return schemaObject
	}
	return map[string]any{
		"description": description,
	}
}

// normalizeComment collapses a raw protobuf source comment into readable text.
//
// Proto source locations preserve comment line wrapping. We keep paragraph
// breaks but otherwise fold each paragraph onto one line so the resulting text
// reads naturally in MCP tool descriptions and JSON Schema fields.
func normalizeComment(comment string) string {
	comment = protoCommentMetadataSuffixPattern.ReplaceAllString(comment, "")
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return ""
	}
	paragraphs := strings.Split(comment, "\n\n")
	normalized := make([]string, 0, len(paragraphs))
	for _, paragraph := range paragraphs {
		words := strings.Fields(paragraph)
		if len(words) == 0 {
			continue
		}
		normalized = append(normalized, strings.Join(words, " "))
	}
	return strings.Join(normalized, "\n\n")
}

// protoDocs indexes proto source comments by fully-qualified descriptor name.
type protoDocs struct {
	methods  map[string]string
	messages map[string]string
	fields   map[string]string
}

// newProtoDocs allocates an empty comment index for the parsed API protos.
func newProtoDocs() *protoDocs {
	return &protoDocs{
		methods:  make(map[string]string),
		messages: make(map[string]string),
		fields:   make(map[string]string),
	}
}

// indexFileProto records the service, message, and field comments from one
// parsed proto file.
func (d *protoDocs) indexFileProto(file *descriptorpb.FileDescriptorProto) {
	filePackage := file.GetPackage()
	sourceInfo := file.GetSourceCodeInfo()
	for serviceIndex, service := range file.GetService() {
		serviceName := joinFullName(filePackage, service.GetName())
		for methodIndex, method := range service.GetMethod() {
			if comment := commentForPath(sourceInfo, 6, int32(serviceIndex), 2, int32(methodIndex)); comment != "" {
				d.methods[joinFullName(serviceName, method.GetName())] = comment
			}
		}
	}
	for messageIndex, message := range file.GetMessageType() {
		d.indexMessageProto(filePackage, message, sourceInfo, 4, int32(messageIndex))
	}
}

// indexMessageProto records comments for one message, its fields, and its
// nested message types.
func (d *protoDocs) indexMessageProto(parentName string, message *descriptorpb.DescriptorProto, sourceInfo *descriptorpb.SourceCodeInfo, path ...int32) {
	messageName := joinFullName(parentName, message.GetName())
	if comment := commentForPath(sourceInfo, path...); comment != "" {
		d.messages[messageName] = comment
	}
	for fieldIndex, field := range message.GetField() {
		if comment := commentForPath(sourceInfo, append(path, 2, int32(fieldIndex))...); comment != "" {
			d.fields[joinFullName(messageName, field.GetName())] = comment
		}
	}
	for nestedIndex, nested := range message.GetNestedType() {
		d.indexMessageProto(messageName, nested, sourceInfo, append(path, 3, int32(nestedIndex))...)
	}
}

// methodComment returns the doc comment for one API RPC, with a generated
// fallback when the proto source has no comment.
func (d *protoDocs) methodComment(method protoreflect.MethodDescriptor) string {
	if comment := d.methods[string(method.FullName())]; comment != "" {
		return comment
	}
	return fmt.Sprintf("Calls BuildBuddy POST /api/v1/%s.", method.Name())
}

// messageComment returns the normalized proto comment for one message
// descriptor, if any.
func (d *protoDocs) messageComment(message protoreflect.MessageDescriptor) string {
	comment := d.messages[string(message.FullName())]
	if !strings.HasSuffix(string(message.Name()), "Request") {
		return comment
	}
	comment = requestMessageCommentPrefixPattern.ReplaceAllString(comment, "")
	return strings.TrimSpace(comment)
}

// fieldComment returns the normalized proto comment for one field descriptor,
// if any.
func (d *protoDocs) fieldComment(field protoreflect.FieldDescriptor) string {
	return d.fields[string(field.FullName())]
}

// joinFullName builds a dotted protobuf full name from a parent scope and a
// child identifier.
func joinFullName(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// commentForPath extracts the normalized comment text attached to one
// descriptor path within a FileDescriptorProto.
func commentForPath(sourceInfo *descriptorpb.SourceCodeInfo, path ...int32) string {
	for _, location := range sourceInfo.GetLocation() {
		if slices.Equal(location.GetPath(), path) {
			return sourceInfoComment(location)
		}
	}
	return ""
}

// sourceInfoComment extracts and normalizes the human-written comment text
// attached to one parsed proto element.
func sourceInfoComment(location *descriptorpb.SourceCodeInfo_Location) string {
	if location == nil {
		return ""
	}
	commentParts := make([]string, 0, len(location.GetLeadingDetachedComments())+1)
	for _, detached := range location.GetLeadingDetachedComments() {
		if text := normalizeComment(detached); text != "" {
			commentParts = append(commentParts, text)
		}
	}
	if text := normalizeComment(location.GetLeadingComments()); text != "" {
		commentParts = append(commentParts, text)
	}
	return strings.Join(commentParts, "\n\n")
}

// valueSchema converts proto field types into the corresponding JSON schema
// representation. The JSON encoding follows ProtoJSON.
func (b *schemaBuilder) valueSchema(field protoreflect.FieldDescriptor) any {
	switch field.Kind() {
	// Primitives
	case protoreflect.BoolKind:
		return map[string]any{
			"type": "boolean",
		}
	case protoreflect.StringKind:
		return map[string]any{
			"type": "string",
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return map[string]any{
			"type": "integer",
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return map[string]any{
			"type": "string",
		}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{
			"type": "number",
		}
	case protoreflect.BytesKind:
		return map[string]any{
			"type":            "string",
			"contentEncoding": "base64",
		}
	// Enums
	case protoreflect.EnumKind:
		if string(field.Enum().FullName()) == "google.protobuf.NullValue" {
			// NullValue: "The JSON representation for `NullValue` is JSON `null`."
			// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/struct.proto
			return map[string]any{
				"type": "null",
			}
		}
		enumValues := make([]string, 0, field.Enum().Values().Len())
		for i := range field.Enum().Values().Len() {
			enumValues = append(enumValues, string(field.Enum().Values().Get(i).Name()))
		}
		return map[string]any{
			"type": "string",
			"enum": enumValues,
		}
	// Messages
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return b.messageValueSchema(field.Message())
	// Other
	default:
		return true // "any" schema
	}
}

// messageValueSchema returns the JSON Schema representation for a protobuf
// message type.
func (b *schemaBuilder) messageValueSchema(desc protoreflect.MessageDescriptor) any {
	switch string(desc.FullName()) {
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/any.proto
	case "google.protobuf.Any":
		return map[string]any{
			"type": "object",
		}
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/empty.proto
	case "google.protobuf.Empty":
		return map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/timestamp.proto
	case "google.protobuf.Timestamp":
		return map[string]any{
			"type":     "string",
			"format":   "date-time",
			"examples": []any{"2024-01-02T03:04:05Z"},
		}

	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/duration.proto
	case "google.protobuf.Duration":
		return map[string]any{
			"type": "string",
			"examples": []any{
				// Only seconds are supported, so give examples demonstrating
				// how to represent very small and very large durations.
				"1s",
				"0.123456789s",
				"60s",
				"3600s",
				"86400s",
			},
		}
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/field_mask.proto
	case "google.protobuf.FieldMask":
		return map[string]any{
			"type":     "string",
			"examples": []any{"scalar_field", "message_field.child_field", "field1,field2,field3"},
		}
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/wrappers.proto
	case "google.protobuf.StringValue":
		return map[string]any{
			"type": "string",
		}
	case "google.protobuf.BoolValue":
		return map[string]any{
			"type": "boolean",
		}
	case "google.protobuf.Int32Value", "google.protobuf.UInt32Value":
		return map[string]any{
			"type": "integer",
		}
	case "google.protobuf.Int64Value", "google.protobuf.UInt64Value":
		return map[string]any{
			"type": "string",
		}
	case "google.protobuf.FloatValue", "google.protobuf.DoubleValue":
		return map[string]any{
			"type": "number",
		}
	case "google.protobuf.BytesValue":
		return map[string]any{
			"type":            "string",
			"contentEncoding": "base64",
		}
	// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/struct.proto
	case "google.protobuf.Struct":
		return map[string]any{
			"type": "object",
		}
	case "google.protobuf.Value":
		return true
	case "google.protobuf.ListValue":
		return map[string]any{
			"type": "array",
		}
	}
	return b.objectSchema(desc)
}
