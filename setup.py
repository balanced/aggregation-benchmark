from setuptools import setup, find_packages

packages = find_packages('.', exclude=('tests', 'tests.*', 'db', 'db.*'))


setup(
    name='benchmark',
    packages=packages,
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLALchemy==0.9.4',
        'pyzmq',
    ],
)
