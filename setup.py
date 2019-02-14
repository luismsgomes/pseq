from setuptools import setup, find_packages
from os import path
import re


def packagefile(*relpath):
    return path.join(path.dirname(__file__), *relpath)


def read(*relpath):
    with open(packagefile(*relpath)) as f:
        return f.read()


def get_version(*relpath):
    match = re.search(
        r'''^__version__ = ['"]([^'"]*)['"]''',
        read(*relpath),
        re.M
    )
    if not match:
        raise RuntimeError('Unable to find version string.')
    return match.group(1)


setup(
    name='pseq',
    version=get_version('src', 'pseq', '__init__.py'),
    description='A framework for parallel processing of sequences.',
    long_description=read('README.rst'),
    url='https://github.com/luismsgomes/pseq',
    author='Luís Gomes',
    author_email='luismsgomes@gmail.com',
    license='GPLv3',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='sequence parallel multiprocessing process subprocess',
    install_requires=[],
    package_dir={'': 'src'},
    packages=find_packages('src'),
)
