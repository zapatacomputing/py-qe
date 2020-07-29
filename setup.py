import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="pyqe",
    version="0.1.0",
    author="Zapata Computing, Inc.",
    author_email="info@zapatacomputing.com",
    description="Supporting utilities for analyzing data from Quantum Engine workflows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zapatacomputing/zmachine",
    packages=setuptools.find_packages(where='src/python'),
    package_dir={'' : 'src/python'},
    scripts=['scripts/qe-sql'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pandas',
        'sqlalchemy',
        'flatten-json==0.1.7',
        'openpyxl'
    ]
)
