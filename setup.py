from setuptools import find_packages, setup

setup(
    name='s3',
    packages=find_packages(include=['s3']),
    version='v1.0.1-alpha',
    description='BRIDGE s3 connector, allows managing s3.',
    author='BRIDGE',
    license='GPL',
    install_requires=[
        'boto3>=1.17.*,<2.0.0'
    ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest==4.4.1',
        'isort',
        'black',
        'coverage',
        'flake8',
        'flake8-print',
        'flake8-debugger',
        'flake8-comprehensions',
        'moto'
    ],
    test_suite='tests',
)
