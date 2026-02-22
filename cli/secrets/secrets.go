package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"golang.org/x/crypto/nacl/box"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
)

var (
	flags  = flag.NewFlagSet("secrets", flag.ContinueOnError)
	target = flags.String("target", login.DefaultApiTarget, "BuildBuddy gRPC target")

	usage = `
usage: bb ` + flags.Name() + ` [--target=<target>] <subcommand> [args]

Manages BuildBuddy org-level encrypted secrets.

Subcommands:
  create <name> --file=FILE
                          Creates a new secret from file contents.
                          Use --file=- to read from stdin.
  update <name> --file=FILE
                          Updates an existing secret from file contents.
                          Use --file=- to read from stdin.
  list                    Lists secret names.
  delete <name>           Deletes a secret.
`
)

type secretsClient interface {
	GetPublicKey(ctx context.Context, req *skpb.GetPublicKeyRequest, opts ...grpc.CallOption) (*skpb.GetPublicKeyResponse, error)
	ListSecrets(ctx context.Context, req *skpb.ListSecretsRequest, opts ...grpc.CallOption) (*skpb.ListSecretsResponse, error)
	UpdateSecret(ctx context.Context, req *skpb.UpdateSecretRequest, opts ...grpc.CallOption) (*skpb.UpdateSecretResponse, error)
	DeleteSecret(ctx context.Context, req *skpb.DeleteSecretRequest, opts ...grpc.CallOption) (*skpb.DeleteSecretResponse, error)
}

func HandleSecrets(args []string) (exitCode int, err error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	cmdAndArgs := flags.Args()
	if len(cmdAndArgs) == 0 {
		log.Print(usage)
		return 1, nil
	}
	if cmdAndArgs[0] == "help" {
		log.Print(usage)
		return 0, nil
	}

	ctx, err := authenticatedContext()
	if err != nil {
		log.Print(err)
		return 1, nil
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Print(err)
		return 1, nil
	}
	defer conn.Close()

	client := bbspb.NewBuildBuddyServiceClient(conn)
	if err := runSubcommand(ctx, client, os.Stdout, cmdAndArgs[0], cmdAndArgs[1:]); err != nil {
		log.Print(err)
		return 1, nil
	}

	return 0, nil
}

func authenticatedContext() (context.Context, error) {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return nil, fmt.Errorf("get API key: %w", err)
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("api key is empty")
	}
	return metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey), nil
}

func runSubcommand(ctx context.Context, client secretsClient, stdout io.Writer, subcommand string, args []string) error {
	switch subcommand {
	case "create":
		return createSecret(ctx, client, stdout, args)
	case "update":
		return updateSecret(ctx, client, stdout, args)
	case "list":
		return listSecrets(ctx, client, stdout, args)
	case "delete":
		return deleteSecret(ctx, client, stdout, args)
	default:
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}
}

func createSecret(ctx context.Context, client secretsClient, stdout io.Writer, args []string) error {
	name, value, err := parseNameAndFileArgs("create", args, os.Stdin)
	if err != nil {
		return err
	}

	existing, err := secretNames(ctx, client)
	if err != nil {
		return err
	}
	if slices.Contains(existing, name) {
		return fmt.Errorf("secret %q already exists; use `bb secrets update`", name)
	}

	if err := encryptAndUpdateSecret(ctx, client, name, value); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "Created secret %q\n", name)
	return err
}

func updateSecret(ctx context.Context, client secretsClient, stdout io.Writer, args []string) error {
	name, value, err := parseNameAndFileArgs("update", args, os.Stdin)
	if err != nil {
		return err
	}

	existing, err := secretNames(ctx, client)
	if err != nil {
		return err
	}
	if !slices.Contains(existing, name) {
		return fmt.Errorf("secret %q not found; use `bb secrets create`", name)
	}

	if err := encryptAndUpdateSecret(ctx, client, name, value); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "Updated secret %q\n", name)
	return err
}

func listSecrets(ctx context.Context, client secretsClient, stdout io.Writer, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: bb secrets list")
	}
	names, err := secretNames(ctx, client)
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, err := fmt.Fprintln(stdout, name); err != nil {
			return err
		}
	}
	return nil
}

func deleteSecret(ctx context.Context, client secretsClient, stdout io.Writer, args []string) error {
	name, err := parseNameArg("delete", args)
	if err != nil {
		return err
	}
	_, err = client.DeleteSecret(ctx, &skpb.DeleteSecretRequest{
		Secret: &skpb.Secret{Name: name},
	})
	if err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
	_, err = fmt.Fprintf(stdout, "Deleted secret %q\n", name)
	return err
}

func parseNameAndFileArgs(subcommand string, args []string, stdin io.Reader) (string, string, error) {
	usage := fmt.Sprintf("usage: bb secrets %s <name> --file=FILE", subcommand)
	subcommandFlags := flag.NewFlagSet(subcommand, flag.ContinueOnError)
	inputFile := subcommandFlags.String("file", "", "Path to file containing the secret value. Use '-' to read from stdin.")
	if err := arg.ParseFlagSet(subcommandFlags, args); err != nil {
		if err == flag.ErrHelp {
			return "", "", errors.New(usage)
		}
		return "", "", err
	}
	if subcommandFlags.NArg() != 1 {
		return "", "", errors.New(usage)
	}
	name := strings.TrimSpace(subcommandFlags.Arg(0))
	if name == "" {
		return "", "", fmt.Errorf("secret name is required")
	}
	if *inputFile == "" {
		return "", "", errors.New(usage)
	}
	value, err := readSecretValue(*inputFile, stdin)
	if err != nil {
		return "", "", err
	}
	if value == "" {
		return "", "", fmt.Errorf("secret value is required")
	}
	return name, value, nil
}

func readSecretValue(dataFile string, stdin io.Reader) (string, error) {
	if dataFile == "-" {
		b, err := io.ReadAll(stdin)
		if err != nil {
			return "", fmt.Errorf("read secret value from stdin: %w", err)
		}
		return string(b), nil
	}

	b, err := os.ReadFile(dataFile)
	if err != nil {
		return "", fmt.Errorf("read secret value file %q: %w", dataFile, err)
	}
	return string(b), nil
}

func parseNameArg(subcommand string, args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("usage: bb secrets %s <name>", subcommand)
	}
	name := strings.TrimSpace(args[0])
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	return name, nil
}

func secretNames(ctx context.Context, client secretsClient) ([]string, error) {
	rsp, err := client.ListSecrets(ctx, &skpb.ListSecretsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}

	names := make([]string, 0, len(rsp.GetSecret()))
	for _, secret := range rsp.GetSecret() {
		names = append(names, secret.GetName())
	}
	slices.Sort(names)
	return names, nil
}

func encryptAndUpdateSecret(ctx context.Context, client secretsClient, name, value string) error {
	keyRsp, err := client.GetPublicKey(ctx, &skpb.GetPublicKeyRequest{})
	if err != nil {
		return fmt.Errorf("get public key: %w", err)
	}
	if keyRsp.GetPublicKey() == nil {
		return fmt.Errorf("server did not return public key")
	}

	encryptedValue, err := encryptSecretValue(keyRsp.GetPublicKey().GetValue(), value)
	if err != nil {
		return err
	}

	_, err = client.UpdateSecret(ctx, &skpb.UpdateSecretRequest{
		Secret: &skpb.Secret{
			Name:  name,
			Value: encryptedValue,
		},
	})
	if err != nil {
		return fmt.Errorf("update secret: %w", err)
	}
	return nil
}

func encryptSecretValue(base64PublicKey, value string) (string, error) {
	pubKeySlice, err := base64.StdEncoding.DecodeString(base64PublicKey)
	if err != nil {
		return "", fmt.Errorf("decode public key: %w", err)
	}
	if len(pubKeySlice) != 32 {
		return "", fmt.Errorf("invalid public key length %d, expected 32", len(pubKeySlice))
	}

	var pubKey [32]byte
	copy(pubKey[:], pubKeySlice)

	// Equivalent to libsodium's crypto_box_seal.
	ciphertextBytes, err := box.SealAnonymous(nil, []byte(value), &pubKey, rand.Reader)
	if err != nil {
		return "", fmt.Errorf("encrypt secret: %w", err)
	}
	return base64.StdEncoding.EncodeToString(ciphertextBytes), nil
}
