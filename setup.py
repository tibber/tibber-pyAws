import sys

from setuptools import setup

install_requires = [
    "aiobotocore",
    "aiohttp",
    "async_timeout",
    "boto3"
]

if sys.version_info < (3, 7):
    install_requires.append("async_exit_stack")

setup(
    name="tibber_aws",
    packages=["tibber_aws"],
    install_requires=install_requires,
    version="0.8.4",
    description="A python3 library to communicate with Aws",
    python_requires=">=3.5.3",
    author="Tibber",
    author_email="drdaniel@tibber.com",
    url="https://github.com/tibbercom/tibber-pyAws",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
