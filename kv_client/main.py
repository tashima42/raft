import grpc
import keyval_pb2
import keyval_pb2_grpc

def main():
    with grpc.insecure_channel('localhost:5431') as channel:
        stub = keyval_pb2_grpc.KeyValStub(channel)
        response = stub.Set(keyval_pb2.Pack(key="country", value="brasil"))
        print("Greeter client received: " + response.value)
        response = stub.Get(keyval_pb2.GetRequest(key='country'))
        print("Greeter client received: " + response.value)


if __name__ == "__main__":
    main()

