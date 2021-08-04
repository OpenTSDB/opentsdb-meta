VERSION=`git describe --tags --abbrev=0`
echo "Docker push version ${VERSION} Login user: $LOGIN_USERNAME"
curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh
docker build . -t opentsdb-meta
echo "Built the image"
docker tag opentsdb-meta:latest opentsdb/opentsdb-meta:$VERSION
echo "Tagged the image."
docker login --username $LOGIN_USERNAME --password $LOGIN_PASSWORD
echo Pushing to opentsdb/opentsdb-meta:$VERSION
docker image push opentsdb/opentsdb-meta:$VERSION
