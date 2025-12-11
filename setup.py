#!/usr/bin/env python3
"""
Setup script for the Raft consensus library
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="raft-consensus",
    version="1.0.0",
    author="Rawan Khalifa",
    description="Production-ready Raft consensus implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Rawan-Khalifa/etcd-raft-kv",
    packages=find_packages(include=["raft", "raft.*", "scripts"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "grpcio>=1.50.0",
        "grpcio-tools>=1.50.0",
        "protobuf>=4.21.0",
        "requests>=2.28.0",
    ],
    entry_points={
        "console_scripts": [
            "raft-cli=scripts.raft_cli:main",
        ],
    },
)
