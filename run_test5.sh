sleep 15
echo -e "5\n0\n" | go run test/main.go
sleep 5
docker compose logs peer1 > log1.txt
docker compose logs peer2 > log2.txt
docker compose logs peer3 > log3.txt
