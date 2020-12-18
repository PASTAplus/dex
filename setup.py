# -*- coding: utf-8 -*-

""":Mod: setup.py

:Synopsis:

:Author:
    servilla

:Created:
    4/12/2020
"""
from os import path
from setuptools import find_packages, setup


here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

with open(path.join(here, "LICENSE"), encoding="utf-8") as f:
    full_license = f.read()

setup(
    name="dex",
    version="2020.04.12",
    description="Manipulate data tables to produce new data tables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="PASTA+ project",
    url="https://github.com/PASTAplus/datapress",
    license=full_license,
    packages=find_packages(where="webapp"),
    include_package_data=True,
    exclude_package_data={"": ["settings.py, properties.py, config.py"],},
    package_dir={"": "webapp"},
    python_requires=">3.8.*",
    install_requires=[
        "click >= 7.1.1",
        "daiquiri >= 2.1.1",
        "numpy >= 1.18.1",
        "pandas >= 1.0.3",
    ],
    entry_points={"console_scripts": ["press=dex.press:main"]},
    classifiers=["License :: OSI Approved :: Apache Software License",],
)


def main():
    return 0


if __name__ == "__main__":
    main()
