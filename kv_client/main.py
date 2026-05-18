import argparse
import grpc
import keyval_pb2
import keyval_pb2_grpc

parser = argparse.ArgumentParser(prog="kv", description="Client for the KeyVal store")
parser.add_argument('-a', '--address', required=True)

subparsers = parser.add_subparsers(dest='command')

parser_get = subparsers.add_parser(name='get')
parser_get.add_argument('key')

parser_set = subparsers.add_parser(name='set')
parser_set.add_argument('key')
parser_set.add_argument('value')

def main():
    args = parser.parse_args()
    try:
        with grpc.insecure_channel(args.address) as channel:
            stub = keyval_pb2_grpc.KeyValStub(channel)
            if args.command == 'set':
                response = stub.Set(keyval_pb2.Pack(key=args.key, value=args.value))
                print("set: " + response.value)
            elif args.command == 'get':
                response = stub.Get(keyval_pb2.GetRequest(key=args.key))
                print("get: " + response.value)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE and "redirect to" in e.details():
            redirect = e.details().split("redirect to")[-1].strip()
            print("Error: this node is not the leader.")
            print("Please retry using the leader address: " + redirect)
            print("  Example: kv -a " + redirect + " " + args.command + " ...")
        else:
            raise


if __name__ == "__main__":
    main()

