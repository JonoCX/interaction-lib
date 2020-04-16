from setuptools import setup, find_packages

name = 'interlib'
version = 0.1
description = 'A library for processing interaction data'
url = 'https://github.com/JonoCX/interaction-lib'
author = 'Jonathan Carlton'
author_email = 'jonathan.carlton@bbc.co.uk'
long_description = description

setup(
    name = name,
    version = version,
    description = description,
    long_description = description,
    url = url,
    author = author,
    author_email = author_email, 
    packages = find_packages(where='interlib'),
    install_requires=[
        'numpy', 'scipy', 'pandas', 'joblib'
    ]
)