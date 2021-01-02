import os
from setuptools import find_packages, setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='redisds',
    version='0.13',
    packages=find_packages(),
    include_package_data=True,
    license='MIT License',  # example license
    description='Python Data structures with persistence in redis',
    long_description='Python data structures with persistence in redis',
    url='https://github.com/ankitml/redis-data-structures-python',
    author='Ankit Mittal',
    author_email='ankitml@gmail.com',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
