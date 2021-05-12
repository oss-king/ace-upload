import pathlib
from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="ace_upload",
    version="1.4.0",
    description="For uploading documents to brewlytics MinIO",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/Oaklea-Simpson-Security/ACE-C",
    author="Oaklea Simpson Security LLC",
    author_email="king@oakleasimpsonsecurity.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=["minio","urllib3","requests","glob2"],
    entry_points={
        "console_scripts": [
            "ace_upload=ace_upload.__main__:main",
        ]
    },
)