## How to run
```
rustc trie.rs && ./trie
```

### GRPC
`cd helloworld-tonic`

**server**
```
cargo run --bin helloworld-server
cargo run --bin helloworld-lb
```

**client**
```
grpcurl -plaintext -import-path ./proto -proto helloworld.proto -d '{"name": "apr", "tenant":"power"}' '127.0.0.1:50051' helloworld.Greeter/SayHello
```

Example response
```
{
  "message": "Hello apr matches: [\"april\"]!"
}
```

Connect to EC2
```
grpcurl -plaintext -import-path ./proto -proto helloworld.proto -d '{"name": "apr"}' 'ec2-54-85-5-194.compute-1.amazonaws.com:50051' helloworld.Greeter/SayHello
```

**EC2**

SSH
```
ssh -i ../dushyant-ec2.pem helloworld-server-image-amd64.tar ec2-user@ec2-54-85-5-194.compute-1.amazonaws.com
```

### Docker

Build and export image from local
```
docker buildx build --platform linux/amd64 -t helloworld-server-image-amd64 .
docker save -o helloworld-server-image-amd64.tar helloworld-server-image-amd64
scp -i ../dushyant-ec2.pem helloworld-server-image-amd64.tar ec2-user@ec2-54-85-5-194.compute-1.amazonaws.com:~
helloworld-server-image-amd64.tar
```

Import and use image on ec2
```
docker load -i helloworld-server-image-amd64.tar
docker run -p 50051:50051 helloworld-server-image-amd64
```

### Other linux commands
Memory usage `free -m`

Run a container image interactively to debug the reason for failure
```
docker run -it --rm helloworld-server-image-amd64 /bin/bash
./target/release/helloworld-server
```

## Todo
0. read dataset from file - Done
1. run this for a large dataset of english words - 370K - Done
2. run as a separate service and call using grpc - DONE
3. Return different prefix and exact matches // test with 'whiplash' - DONE
4. distributed trie
5. UTs, ITs
6. debugging using breakpoint
7. deployment using docker on EC2 - DONE
8. deployment using K8S
9. [Long shot] ubr kind of ranking
10. How does ES compare to this?
11. Persist in redis - OPTIONAL

## details

**Memory**
calculation
10 bytes per words * 370K words = 3.7MB

26 items in array for each character

26*4 MB = 100 MB

Actual memory usage = 700 MB

## Resources
1. https://github.com/kiyoka/distributed-trie
2. https://medium.com/@prefixyteam/how-we-built-prefixy-a-scalable-prefix-search-service-for-powering-autocomplete-c20f98e2eff1 
