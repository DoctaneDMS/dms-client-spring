# Doctane Client for Spring

This module implements the Doctane service api for Java by translating each API
call into a call to the Doctane REST API.

### Running the unit tests

The unit tests expect to find a Doctane REST server on localhost, port 8080. 
Authentication is via a signed service request. Use a gradle.properties file
in the project root to set the location of the keystore file:

localCertsDir=C:\\dev\\doctane\\certs
localKeystore=C:\\dev\\doctane\\doctane.keystore

If these values are the same as are configured for the Doctane REST server, the
tests should run.