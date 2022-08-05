#!/usr/bin/env python3
import collections
import re
import sys
import os

if __name__ == "__main__":
    go_src = sys.argv[1]
    service_package = os.environ['SERVICE_PACKAGE']
    service_package_prefix = service_package.split('/')[-1]

    imports = {}
    rpcs = collections.defaultdict(dict)
    # Scan through the generated `buildbuddy_stream_service.pb.go` file
    # and gather the RPC signatures (RPC name, request type, response type).
    with open(go_src, 'r') as f:
        lines = f.readlines()

        # Find package name
        for line in lines:
            if line.startswith('package '):
                generated_package_name = line.rstrip().split(' ')[1] + "_protolet"
                break

        # Find service name
        # TODO: Allow more than one service
        for line in lines:
            m = re.search(r'type (.*?)Client interface {', line)
            if m:
                service_name = m.group(1)
                break

        if not service_name:
            raise ValueError('no services defined in package %s' % service_package)

        # Find imports
        for line in lines:
            line = line.rstrip()
            m = re.search(r'\t(\w+) "(.*?)"', line)
            if m:
                imports[m.group(1)] = m.group(2)

        # Find request types, looking for patterns like:
        #     Foo(ctx context.Context, in *foo.FooRequest, ...opt grpc.CallOptions)
        for line in lines:
            m = re.search(r'^\s+(\w+)\(ctx context.Context, in (.*?),', line)
            if m:
                name = m.group(1)
                rpcs[name]["request"] = m.group(2)

        # Find response types, looking for patterns like:
        #     type BuildBuddyStreamService_FooServer interface {
        #         ...
        #         Send(*foo.FooResponse)
        #     }
        for (i, line) in enumerate(lines):
            m = re.search(service_name + r'_(.*?)Server interface {', line)
            if not m:
                continue
            rpc_name = m.group(1)
            j = i
            response_type = None
            while j < len(lines) - 2:
                # Scan ahead for Send func
                j += 1
                line = lines[j]
                m = re.search(r'Send\((.*)\)', line)
                if not m:
                    continue
                response_type = m.group(1)
                break
            else:
                raise ValueError('Could not locate Send(...) method for %s RPC client interface' % rpc_name)
            rpcs[rpc_name]["response"] = response_type

    # Build up the set of packages that need to be imported because they appear
    # as a request or response message type.
    message_prefixes = set()
    for (rpc, types) in rpcs.items():
        for side in ('request', 'response'):
            message_type = types[side]
            if not message_type:
                raise ValueError('missing %s type for RPC %s' % (side, rpc))
            prefix = message_type[1:].split('.')[0]
            if prefix not in imports:
                raise ValueError('missing import for "%s" (for %s %s %s)' % (prefix, rpc, side, message_type))
            message_prefixes.add(prefix)

    server_type = '%s.%sServer' % (service_package_prefix, service_name)

    print("package %s" % (generated_package_name))
    print()
    print('import (')
    print('\t"context"')
    print()
    print('\t"github.com/buildbuddy-io/buildbuddy/server/http/protolet"')
    print('\t"google.golang.org/protobuf/proto"')
    print('\t"google.golang.org/grpc"')
    print()
    print('\t%s "%s"' % (service_package_prefix, service_package))
    for prefix in message_prefixes:
        print('\t%s "%s"' % (prefix, imports[prefix]))
    print(')')
    print()
    print('type %sServer struct{' % (service_name))
    print('\timpl %s' % (server_type))
    print('}')
    print()
    print('func New%sServer(impl %s) protolet.Server {' % (service_name, server_type))
    print('\treturn &%sServer{impl}' % (service_name))
    print('}')
    print()
    print('func (s *%sServer) Handle(ctx context.Context, rpc string, req proto.Message, w *protolet.StreamResponseWriter) error {' % (service_name))
    print('\tswitch (rpc) {')
    for (rpc, types) in rpcs.items():
        print('\tcase "%s":' % (rpc))
        print('\t\tstream := %s_%s_Stream{ctx: ctx, w: w}' % (service_name, rpc))
        print('\t\treturn s.impl.%s(req.(%s), stream)' % (rpc, types['request']))
    print('\tdefault:')
    print('\t\treturn protolet.ErrUnknownRPC')
    print('\t}')
    print('}')
    print()
    print('func (*%sServer) NewRequestMessage(rpc string) proto.Message {' % (service_name))
    print('\tswitch (rpc) {')
    for (rpc, types) in rpcs.items():
        print('\tcase "%s":' % (rpc))
        print('\t\treturn %s{}' % (types['request'].replace('*', '&')))
    print('\tdefault:')
    print('\t\treturn nil')
    print('\t}')
    print('}')
    print()
    for (rpc, types) in rpcs.items():
        print('type %s_%s_Stream struct {' % (service_name, rpc))
        print('\tgrpc.ServerStream')
        print('\tctx context.Context')
        print('\tw *protolet.StreamResponseWriter')
        print('}')
        print()
        print('func (x %s_%s_Stream) Context() context.Context {' % (service_name, rpc))
        print('\treturn x.ctx')
        print('}')
        print()
        print('func (x %s_%s_Stream) Send(rsp %s) error {' % (service_name, rpc, types['response']))
        print('\treturn x.w.WriteResponse(rsp)')
        print('}')
        print()
