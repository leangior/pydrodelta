from setuptools import setup, find_packages
import os
import io

def read(filename, encoding='utf-8'):
    """read file contents"""
    full_path = os.path.join(os.path.dirname(__file__), filename)
    with io.open(full_path, encoding=encoding) as fh:
        contents = fh.read().strip()
    return contents

DESCRIPTION = "pydrodelta suite for hydrological modeling"

KEYWORDS = ["analysis","simulation","hydrology","modeling"]

setup(
    name='pydrodelta',
    version="0.1.0",
    description=DESCRIPTION.strip(),
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    license='MIT',
    platforms='all',
    keywords=' '.join(KEYWORDS),
    author='Juan F. Bianchi',
    author_email='jbianchi@ina.gob.ar',
    maintainer='Juan F. Bianchi',
    maintainer_email='jbianchi@ina.gob.ar',
    url='https://github.com/jbianchi81/pydrodelta',
    install_requires=read('requirements.txt').splitlines(),
    packages=find_packages(),
    package_data={'pydrodelta': ['data/schemas/topology.yml','data/schemas/plan.yml','data/schemas/serie.yml']},
    entry_points={
        'console_scripts': [
            'pydrodelta=pydrodelta:cli'
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Hydrology',
        'Topic :: Scientific/Engineering :: GIS'
    ]
)
