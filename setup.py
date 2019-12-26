import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="findopendata",
    version="1.0.3",
    author="Eric Zhu",
    author_email="",
    description="A search engine for Open Data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/findopendata/findopendata",
    keywords='open-data search-engine',
    packages=setuptools.find_packages(exclude=["tests*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'requests>=2.22.0',
        'google-cloud-storage>=1.17.0',
        'azure-storage>=0.36.0',
        'google-auth>=1.6.3',
        'gcsfs>=0.3.0',
        'celery>=4.3.0',
        'psycopg2-binary>=2.7.5',
        'Django>=2.2.3',
        'rfc6266-content-disposition>=0.0.6',
        'simplejson>=3.16.0',
        'genson>=1.1.0',
        'fastavro>=0.22.3',
        'python-snappy>=0.5.4',
        'python-dateutil>=2.8.0',
        'datasketch>=1.4.10',
        'pyfarmhash>=0.2.2',
        'cchardet>=2.1.4',
        'spacy>=2.1.8',
        'beautifulsoup4>=4.8.0',
        'pyyaml>=5.1.2',
        'gevent>=1.4.0',
    ],
    extras_require={
        'test': [
            'nose2>=0.9.1',
        ]
    },
    scripts = [
        'harvest_datasets.py',
        'generate_metadata.py',
        'sketch_dataset_content.py',
    ],
)
