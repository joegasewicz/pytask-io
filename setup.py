from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="forestmq",
    version="0.0.1",
    description="Python Async Queue",
    packages=['forestmq'],
    install_requires=[
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/joegasewicz/forestmq",
    author="Joe Gasewicz",
    author_email="joegasewicz@gmail.com",
)
