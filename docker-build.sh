docker build . -t bridge-mosquitto:latest

docker run --rm -v ~/repos/bridge-mosquitto/certs:/etc/mosquitto/certs bridge-mosquitto:latest
