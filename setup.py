#!/usr/bin/env python
from setuptools import setup, find_packages
from kademLAN import version

setup(
    name="kademLAN",
    version=version,
    description="Kademlia is a distributed hash table for decentralized peer-to-peer computer networks.",
    author="Brian Muller",
    author_email="bamuller@gmail.com",
    license="MIT",
    url="http://github.com/bmuller/kademLAN",
    packages=find_packages(),
    requires=["twisted", "rpcudp", 'pyre'],
    install_requires=['twisted>=14.0', "rpcudp>=1.0"]
)
