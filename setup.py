from setuptools import setup

install_requires = [
    "aioboto3>=9.3.0",
    "aiohttp",
    "async_timeout",
]


setup(
    name="tibber_aws",
    packages=["tibber_aws"],
    install_requires=install_requires,
    version="0.10.0",
    description="A python3 library to communicate with Aws",
    python_requires=">=3.7.0",
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
