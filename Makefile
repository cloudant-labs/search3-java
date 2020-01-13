all: ca.pem
	@mvn clean test

ca.pem:
	@certstrap init --common-name somewhere.over.the.rainbow --passphrase ""
	@certstrap request-cert -ip 127.0.0.1 --passphrase "" -domain localhost
	@certstrap sign localhost --CA somewhere.over.the.rainbow --passphrase ""
	@openssl pkcs8 -topk8 -nocrypt -in out/localhost.key -out key.pem
	@cp out/localhost.crt cert.pem
	@cp out/somewhere.over.the.rainbow.crt ca.pem

clean:
	@rm -f *.pem
	@rm -rf out/
