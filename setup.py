from setuptools import setup

setup(
    name='RedLine',
    version='0.1',
    long_description=__doc__,
    packages=['redline'],
    include_package_data=True,
    zip_safe=False,
    install_requires=['redis>=2.2.0'],
    scripts=['bin/redis-import']
)
