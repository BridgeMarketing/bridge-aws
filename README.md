# s3-connector
Useful functions for communicating with S3, written in python 3.9.

## Including in your application:
In your 'requirements.txt' (your pip install file):
`-e git+https://github.com/BridgeMarketing/s3-connector.git@v0.0.2-alpha#egg=s3_connector`
(Double check that the version after `git@` is the version you want.)

After pip has successfully installed the useful object can be accessed with:
`from s3_connector_lib import S3Connector` or `from s3_connector_lib.s3connector import S3Connector`

## Running the tests:
If you want to run the included test suite run:
`python -m pytest` for a simple test run.
`coverage run -m pytest; coverage report -m s3_connector_lib/*.py ` for a test run and coverate report.

## About
S3Connector is meant to be a collection of convenience functions and a unification of multiple approaches for interacting with S3. If you find yourself repeatedly performing the same steps, and there isn't a function in the library for it, consider contributing that logic, you never know who else might find it useful.

## Contributing
If you are going to add code to the S3Connector, conform to pep8 standards, and be thorough in typing, it can be very useful to those that make use of the library.
