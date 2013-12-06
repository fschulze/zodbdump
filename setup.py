from setuptools import setup


setup(
    name='zodbdump',
    packages=['zodbdump'],
    entry_points={
        'console_scripts': [
            'dump=zodbdump:main']},
    install_requires=[
        'ZODB3'])