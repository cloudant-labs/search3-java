ca.pem:
	@certstrap init --common-name localhost --passphrase ""
	@certstrap request-cert -ip 127.0.0.1 --passphrase ""
	@certstrap sign 127.0.0.1 --CA localhost --passphrase ""
	@openssl pkcs8 -topk8 -nocrypt -in out/127.0.0.1.key -out key.pem
	@cp out/127.0.0.1.crt cert.pem
	@cp out/localhost.crt ca.pem

clean:
	@rm -rf out/

