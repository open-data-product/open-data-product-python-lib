from setuptools import setup, find_packages

setup(
    name="open-data-product-python-lib",
    version="0.1.0",
    description="Python library to build data products",
    author="Open Data Product",
    author_email="opendataproduct@gmail.com",
    packages=find_packages(),
    install_requires=[
        "dacite>=1.9.2",
        "openpyxl>=3.1.5",
        "pandas>=2.3.0",
        "pyyaml>=6.0.2",
        "requests>=2.32.3",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
