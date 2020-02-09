from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="pytask-io",
    version="0.0.2",
    description="An asynchronous Tasks Library using asyncio",
    packages=['pytask_io'],
    install_requires=[
        'redis',
        'dill'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/joegasewicz/pytask_io",
    author="Joe Gasewicz",
    author_email="joegasewicz@gmail.com",
)
