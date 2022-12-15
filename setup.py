from setuptools import setup

requirements = []
with open("requirements.txt") as f:
    for line in f.read().splitlines():
        if line.startswith("#"):
            continue
        requirements.append(line)

setup(
    name="tibber_aws",
    packages=["tibber_aws"],
    install_requires=requirements,
    version="0.15.0",
    description="A python3 library to communicate with Aws",
    python_requires=">=3.7.0",
    author="Tibber",
    author_email="drdaniel@tibber.com",
    url="https://github.com/tibber/tibber-pyAws",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
