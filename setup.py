from setuptools import setup, find_packages
from codecs import open

setup(
    name = 'halt',
    version='0.1.0',
    description = 'very light wieght db helper for sqlite, has some small mashconfig magic',
    author='timothy eichler',
    author_email='tim_eichler@hotmail.com',
    license='BSD',
    classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 3'],

    keywords = 'sqlite db helper lightweight sql magic',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
)
