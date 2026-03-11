package mcptools

import "context"

func (s *Service) getInvocation(ctx context.Context, args map[string]any) (any, error) {
	invocationRef, err := requiredString(args, "invocation_id")
	if err != nil {
		return nil, err
	}
	invocationID, err := extractInvocationID(invocationRef)
	if err != nil {
		return nil, err
	}

	inv, err := s.fetchInvocation(ctx, invocationID)
	if err != nil {
		return nil, err
	}
	return invocationMetadata(inv), nil
}
