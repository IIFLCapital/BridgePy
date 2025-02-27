from setuptools import setup, find_packages

setup(
    name="bridePy",
    version="0.1.0",
    install_requires=[
        "paho-mqtt>=2.1.0",
        "asyncio>=3.4.3"
    ],
)