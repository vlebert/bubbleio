#! python3  # noqa: E265

from setuptools import find_packages, setup

# package (to get version)
from bubblepy import __about__

setup(
    name="bubbleio",
    version=__about__.__version__,
    author=__about__.__author__,
    author_email=__about__.__email__,
    description=__about__.__summary__,
    py_modules=["sqlitetoairtable"],
    # packaging
    packages=find_packages(
        exclude=["contrib", "docs", "*.tests", "*.tests.*", "tests.*", "tests"]
    ),
    include_package_data=True,
    install_requires=[
        "requests",
    ],
)
