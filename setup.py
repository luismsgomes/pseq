from setuptools import setup

# https://setuptools.readthedocs.io/en/latest/
setup(
    name="pseq",
    version="2.0.0",
    description="A framework for parallel processing of sequences.",
    long_description=open("README.rst", "rt").read(),
    url="https://github.com/luismsgomes/pseq",
    author="Luís Gomes",
    author_email="luismsgomes@gmail.com",
    license="GPLv3",
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="sequence parallel multiprocessing process subprocess",
    install_requires=[],
    package_dir={"": "src"},
    py_modules=["pseq"],
)
