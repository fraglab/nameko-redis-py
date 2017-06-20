from setuptools import setup, find_packages

from nameko_redis_utils import __version__


setup(
    name='nameko-redis-utils',
    version=__version__,
    author='Sergey Suglobov',
    author_email='s.suglobov@gmail.com',
    packages=find_packages(),
    keywords="nameko, redis, redis-utils",
    url='https://github.com/fraglab/nameko-redis-utils',
    description='Redis dependency and utils for Nameko',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only'
    ],
    install_requires=[
        'setuptools',
        'nameko',
        'redis',
    ]
)
